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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.socksx.v4.Socks4CmdRequest;
import io.netty.handler.codec.socksx.v4.Socks4CmdRequestDecoder;
import io.netty.handler.codec.socksx.v4.Socks4CmdResponse;
import io.netty.handler.codec.socksx.v4.Socks4CmdStatus;
import io.netty.handler.codec.socksx.v4.Socks4CmdType;
import io.netty.handler.codec.socksx.v4.Socks4MessageEncoder;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class Socks4ProxyServer extends ProxyServer {

    public Socks4ProxyServer(boolean useSsl, TestMode testMode, InetSocketAddress destination) {
        super(useSsl, testMode, destination);
    }

    public Socks4ProxyServer(
            boolean useSsl, TestMode testMode, InetSocketAddress destination, String username) {
        super(useSsl, testMode, destination, username, null);
    }

    @Override
    protected void configure(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        switch (testMode) {
        case TERMINAL:
            p.addLast(new Socks4CmdRequestDecoder());
            p.addLast(Socks4MessageEncoder.INSTANCE);
            p.addLast(new Socks4TerminalHandler());
            break;
        case UNRESPONSIVE:
            p.addLast(UnresponsiveHandler.INSTANCE);
            break;
        }
    }

    private final class Socks4TerminalHandler extends TerminalHandler {
        @Override
        protected boolean handleProxyProtocol(ChannelHandlerContext ctx, Object msg) throws Exception {
            Socks4CmdRequest req = (Socks4CmdRequest) msg;
            assertThat(req.cmdType(), is(Socks4CmdType.CONNECT));

            ctx.pipeline().addBefore(ctx.name(), "lineDecoder", new LineBasedFrameDecoder(64, false, true));

            boolean authzSuccess;
            if (username != null) {
                authzSuccess = username.equals(req.userId());
            } else {
                authzSuccess = true;
            }

            Socks4CmdResponse res;
            boolean sendGreeting = false;
            if (!authzSuccess) {
                res = new Socks4CmdResponse(Socks4CmdStatus.IDENTD_AUTH_FAILURE);
            } else if (!req.host().equals(destination.getHostString()) ||
                       req.port() != destination.getPort()) {
                res = new Socks4CmdResponse(Socks4CmdStatus.REJECTED_OR_FAILED);
            } else {
                res = new Socks4CmdResponse(Socks4CmdStatus.SUCCESS);
                sendGreeting = true;
            }

            ctx.write(res);
            ctx.pipeline().remove(Socks4MessageEncoder.class);

            if (sendGreeting) {
                ctx.write(Unpooled.copiedBuffer("0\n", CharsetUtil.US_ASCII));
            }

            return true;
        }
    }
}
