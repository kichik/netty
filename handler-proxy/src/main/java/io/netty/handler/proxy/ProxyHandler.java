/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.handler.proxy;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.PendingWriteQueue;

import java.net.SocketAddress;
import java.nio.channels.ConnectionPendingException;

public abstract class ProxyHandler extends ChannelDuplexHandler {

    /**
     * Not connected to the destination. This handler is supposed to:
     * <ul>
     * <li>intercept the {@code connect()} operation</li>
     * <li>send the initial message to the proxy server when connected to the proxy server</li>
     * <li>handle the negotiation or authentication with the proxy server</li>
     * <li>connect to the destination</li>
     * <li>hide the {@code channelReadComplete()} events triggered during the communication with the proxy server</li>
     * </ul>
     */
    private static final int ST_NOT_CONNECTED = 0;

    /**
     * Connected to the destination successfully.  This handler is supposed to:
     * <ul>
     * <li>hide the last {@code channelReadComplete()} event triggered during the communication with the proxy
     *     server, and move on to the {@link #ST_FINISHED} state</li>
     * </ul>
     */
    private static final int ST_CONNECTED = 1;

    /** There's nothing left to do in this handler. */
    private static final int ST_FINISHED = 2;

    private final SocketAddress proxyAddress;
    private volatile SocketAddress destinationAddress;

    private PendingWriteQueue pendingWrites;
    @SuppressWarnings("RedundantFieldInitialization")
    private int state = ST_NOT_CONNECTED;
    private boolean flushedPrematurely;

    protected ProxyHandler(SocketAddress proxyAddress) {
        if (proxyAddress == null) {
            throw new NullPointerException("proxyAddress");
        }
        this.proxyAddress = proxyAddress;
    }

    /**
     * Returns the name of the proxy protocol in use.
     */
    public abstract String protocol();

    /**
     * Returns the name of the authentication scheme in use.
     */
    public abstract String authScheme();

    @SuppressWarnings("unchecked")
    public final <T extends SocketAddress> T proxyAddress() {
        return (T) proxyAddress;
    }

    @SuppressWarnings("unchecked")
    public final <T extends SocketAddress> T destinationAddress() {
        return (T) destinationAddress;
    }

    public final boolean isConnected() {
        return state != ST_NOT_CONNECTED;
    }

    @Override
    public final void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive()) {
            // channelActive() event has been fired already, which means this.channelActive() will
            // not be invoked. We have to initialize here instead.
            sendInitialMessage(ctx);
        } else {
            // channelActive() event has not been fired yet.  this.channelOpen() will be invoked
            // and initialization will occur there.
        }

        configurePipeline(ctx);
    }

    protected abstract void configurePipeline(ChannelHandlerContext ctx) throws Exception;

    @Override
    public final void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        deconfigurePipeline(ctx);
    }

    protected abstract void deconfigurePipeline(ChannelHandlerContext ctx) throws Exception;

    @Override
    public final void connect(
            ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {

        if (destinationAddress != null) {
            promise.setFailure(new ConnectionPendingException());
            return;
        }

        destinationAddress = remoteAddress;
        ctx.connect(proxyAddress, localAddress, promise);
    }

    @Override
    public final void channelActive(ChannelHandlerContext ctx) throws Exception {
        sendInitialMessage(ctx);
        ctx.fireChannelActive();
    }

    private void sendInitialMessage(ChannelHandlerContext ctx) throws Exception {
        // TODO: Handle response timeout properly.
        Object initialMessage = newInitialMessage(ctx);
        if (initialMessage != null) {
            ctx.writeAndFlush(initialMessage);
        }
    }

    protected abstract Object newInitialMessage(ChannelHandlerContext ctx) throws Exception;

    @Override
    public final void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        switch (state) {
        case ST_NOT_CONNECTED:
            boolean success = false;
            try {
                boolean done = handleResponse(ctx, msg);
                if (done) {
                    setConnected(ctx);
                }
                success = true;
            } finally {
                if (!success) {
                    ctx.close();
                }
            }
            break;
        case ST_CONNECTED:
            // Received a message after the connection has been established *and* before channelReadComplete().
            // We should not let our channelReadComplete() handler method swallow a channelReadComplete() event,
            // because otherwise a user will not see the channelReadComplete() event for his or her message.
            state = ST_FINISHED;
        case ST_FINISHED:
            // Received a message after the connection has been established; pass through.
            ctx.fireChannelRead(msg);
            break;
        }
    }

    private void setConnected(ChannelHandlerContext ctx) {
        state = ST_CONNECTED;
        clearPendingWrites();

        if (flushedPrematurely) {
            ctx.flush();
        }
    }

    protected abstract boolean handleResponse(ChannelHandlerContext ctx, Object response) throws Exception;

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        switch (state) {
        case ST_NOT_CONNECTED:
            readIfNecessary(ctx);
            break;
        case ST_CONNECTED:
            // The last channelReadComplete() event triggered during the communication with the proxy server.
            state = ST_FINISHED;
            readIfNecessary(ctx);
            break;
        case ST_FINISHED:
            ctx.fireChannelReadComplete();
            break;
        }
    }

    private static void readIfNecessary(ChannelHandlerContext ctx) {
        if (!ctx.channel().config().isAutoRead()) {
            ctx.read();
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (state == ST_NOT_CONNECTED) {
            addPendingWrite(ctx, msg, promise);
            return;
        }

        clearPendingWrites();
        ctx.write(msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (state == ST_NOT_CONNECTED) {
            flushedPrematurely = true;
            return;
        }

        clearPendingWrites();
        ctx.flush();
    }

    private void clearPendingWrites() {
        if (pendingWrites != null) {
            pendingWrites.removeAndWriteAll();
            pendingWrites = null;
        }
    }

    private void addPendingWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        PendingWriteQueue pendingWrites = this.pendingWrites;
        if (pendingWrites == null) {
            this.pendingWrites = pendingWrites = new PendingWriteQueue(ctx);
        }
        pendingWrites.add(msg, promise);
    }
}
