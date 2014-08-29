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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.NetUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class ProxyServer {

    protected final InternalLogger logger = InternalLoggerFactory.getInstance(getClass());

    private final ServerSocketChannel ch;
    private final Queue<Throwable> recordedExceptions = new LinkedBlockingQueue<Throwable>();
    protected final TestMode testMode;
    protected final String username;
    protected final String password;
    protected final InetSocketAddress destination;

    /**
     * Starts a new proxy server with disabled authentication for testing purpose.
     *
     * @param useSsl {@code true} if and only if implicit SSL is enabled
     * @param testMode the test mode
     * @param destination the expected destination.
     *                    If the client requests proxying to a different destination,
     *                    this server will reject the connection request.
     */
    protected ProxyServer(boolean useSsl, TestMode testMode, InetSocketAddress destination) {
        this(useSsl, testMode, destination, null, null);
    }

    /**
     * Starts a new proxy server with disabled authentication for testing purpose.
     *
     * @param useSsl {@code true} if and only if implicit SSL is enabled
     * @param testMode the test mode
     * @param username the expected username.
     *                 If the client tries to authenticate with a different username, this server will fail the
     *                 authentication request.
     * @param password the expected password.
     *                 If the client tries to authenticate with a different password, this server will fail the
     *                 authentication request.
     * @param destination the expected destination.
     *                    If the client requests proxying to a different destination,
     *                    this server will reject the connection request.
     */
    protected ProxyServer(
            final boolean useSsl, TestMode testMode,
            InetSocketAddress destination, String username, String password) {

        this.testMode = testMode;
        this.destination = destination;
        this.username = username;
        this.password = password;

        ServerBootstrap b = new ServerBootstrap();
        b.channel(NioServerSocketChannel.class);
        b.group(ProxyHandlerTest.group);
        b.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                if (useSsl) {
                    p.addLast(ProxyHandlerTest.serverSslCtx.newHandler(ch.alloc()));
                }

                configure(ch);
            }
        });

        ch = (ServerSocketChannel) b.bind(NetUtil.LOCALHOST, 0).syncUninterruptibly().channel();
    }

    public InetSocketAddress address() {
        return new InetSocketAddress(NetUtil.LOCALHOST, ch.localAddress().getPort());
    }

    protected abstract void configure(SocketChannel ch) throws Exception;

    protected void recordException(Throwable t) {
        logger.warn("Unexpected exception from proxy server:", t);
        recordedExceptions.add(t);
    }

    /**
     * Clears all recorded exceptions.
     */
    public void clearExceptions() {
        recordedExceptions.clear();
    }

    /**
     * Logs all recorded exceptions and raises the last one so that the caller can fail.
     */
    public void checkExceptions() {
        Throwable t;
        for (;;) {
            t = recordedExceptions.poll();
            if (t == null) {
                break;
            }

            logger.warn("Unexpected exception:", t);
        }

        if (t != null) {
            PlatformDependent.throwException(t);
        }
    }

    public final void stop() {
        ch.close();
    }
}
