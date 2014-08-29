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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class HttpProxyServer extends ProxyServer {

    public HttpProxyServer(boolean useSsl, TestMode testMode, InetSocketAddress destination) {
        super(useSsl, testMode, destination);
    }

    public HttpProxyServer(
            boolean useSsl, TestMode testMode, InetSocketAddress destination, String username, String password) {
        super(useSsl, testMode, destination, username, password);
    }

    @Override
    protected void configure(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        switch (testMode) {
        case TERMINAL:
            p.addLast(new HttpServerCodec());
            p.addLast(new HttpObjectAggregator(1));
            p.addLast(new TerminalHandler());
            break;
        case UNRESPONSIVE:
            p.addLast(UnresponsiveHandler.INSTANCE);
            break;
        }
    }

    private final class TerminalHandler extends SimpleChannelInboundHandler<Object> {

        private boolean finished;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (finished) {
                String str = ((ByteBuf) msg).toString(CharsetUtil.US_ASCII);
                if ("A\n".equals(str)) {
                    ctx.write(Unpooled.copiedBuffer("1\n", CharsetUtil.US_ASCII));
                } else if ("B\n".equals(str)) {
                    ctx.write(Unpooled.copiedBuffer("2\n", CharsetUtil.US_ASCII));
                } else if ("C\n".equals(str)) {
                    ctx.write(Unpooled.copiedBuffer("3\n", CharsetUtil.US_ASCII))
                       .addListener(ChannelFutureListener.CLOSE);
                } else {
                    throw new IllegalStateException("unexpected message: " + str);
                }
                return;
            }

            FullHttpRequest req = (FullHttpRequest) msg;
            assertThat(req.method(), is(HttpMethod.CONNECT));

            finished = true;

            ctx.pipeline().addBefore(ctx.name(), "lineDecoder", new LineBasedFrameDecoder(64, false, true));
            ctx.pipeline().remove(HttpObjectAggregator.class);
            ctx.pipeline().remove(HttpRequestDecoder.class);

            boolean authzSuccess = false;
            if (username != null) {
                String authz = req.headers().get(Names.AUTHORIZATION);
                if (authz != null) {
                    ByteBuf authzBuf64 = Unpooled.copiedBuffer(authz, CharsetUtil.US_ASCII);
                    ByteBuf authzBuf = Base64.decode(authzBuf64);
                    authz = authzBuf.toString(CharsetUtil.US_ASCII);
                    authzBuf64.release();
                    authzBuf.release();
                    String expectedAuthz = username + ':' + password;
                    authzSuccess = expectedAuthz.equals(authz);
                }
            } else {
                authzSuccess = true;
            }

            FullHttpResponse res;
            boolean sendGreeting = false;
            if (!authzSuccess) {
                res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
                res.headers().set(Names.CONTENT_LENGTH, 0);
            } else if (!req.uri().equals(destination.getHostString() + ':' + destination.getPort())) {
                res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.FORBIDDEN);
                res.headers().set(Names.CONTENT_LENGTH, 0);
            } else {
                res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
                sendGreeting = true;
            }

            ctx.write(res);
            ctx.pipeline().remove(HttpResponseEncoder.class);

            if (sendGreeting) {
                ctx.write(Unpooled.copiedBuffer("0\n", CharsetUtil.US_ASCII));
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            recordException(cause);
            ctx.close();
        }
    }
}
