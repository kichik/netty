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
import io.netty.handler.codec.AsciiString;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public final class Http1ProxyHandler extends ProxyHandler {

    private final String username;
    private final String password;
    private final CharSequence authorization;

    public Http1ProxyHandler(SocketAddress proxyAddress) {
        super(proxyAddress);
        username = null;
        password = null;
        authorization = null;
    }

    public Http1ProxyHandler(SocketAddress proxyAddress, String username, String password) {
        super(proxyAddress);
        if (username == null) {
            throw new NullPointerException("username");
        }
        if (password == null) {
            throw new NullPointerException("password");
        }
        this.username = username;
        this.password = password;
        authorization = new AsciiString(
                Base64.encode(Unpooled.copiedBuffer(username + ':' + password, CharsetUtil.UTF_8), false).nioBuffer());
    }

    @Override
    public String protocol() {
        return "http/1";
    }

    @Override
    public String authScheme() {
        return authorization != null? "basic" : "none";
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    @Override
    protected void configurePipeline(ChannelHandlerContext ctx) throws Exception {
        ChannelPipeline p = ctx.pipeline();
        p.addBefore(ctx.name(), "http1decoder", new HttpResponseDecoder());
        p.addBefore(ctx.name(), "http1encoder", new HttpRequestEncoder());
    }

    @Override
    protected void deconfigurePipeline(ChannelHandlerContext ctx) throws Exception {
        ChannelPipeline p = ctx.pipeline();
        p.remove("http1decoder");
        p.remove("http1encoder");
    }

    @Override
    protected Object newInitialMessage(ChannelHandlerContext ctx) throws Exception {
        InetSocketAddress raddr = destinationAddress();
        String rhost;
        if (raddr.isUnresolved()) {
            rhost = raddr.getHostString();
        } else {
            rhost = raddr.getAddress().getHostAddress();
        }

        FullHttpRequest req = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.CONNECT, rhost + ':' + raddr.getPort(),
                Unpooled.EMPTY_BUFFER, false);

        if (authorization != null) {
            req.headers().set(Names.AUTHORIZATION, authorization);
        }

        return req;
    }

    @Override
    protected boolean handleResponse(ChannelHandlerContext ctx, Object response) throws Exception {
        if (response instanceof HttpResponse) {
            HttpResponse res = (HttpResponse) response;
            if (res.status().code() != 200) {
                throw new ProxyConnectException("proxyAddress: " + proxyAddress() + ", response: " + res);
            }
        }

        return response instanceof LastHttpContent;
    }
}
