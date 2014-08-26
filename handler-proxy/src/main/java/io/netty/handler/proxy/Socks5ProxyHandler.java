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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.socksx.v5.Socks5AddressType;
import io.netty.handler.codec.socksx.v5.Socks5AuthRequest;
import io.netty.handler.codec.socksx.v5.Socks5AuthResponse;
import io.netty.handler.codec.socksx.v5.Socks5AuthResponseDecoder;
import io.netty.handler.codec.socksx.v5.Socks5AuthScheme;
import io.netty.handler.codec.socksx.v5.Socks5AuthStatus;
import io.netty.handler.codec.socksx.v5.Socks5CmdRequest;
import io.netty.handler.codec.socksx.v5.Socks5CmdResponse;
import io.netty.handler.codec.socksx.v5.Socks5CmdResponseDecoder;
import io.netty.handler.codec.socksx.v5.Socks5CmdStatus;
import io.netty.handler.codec.socksx.v5.Socks5CmdType;
import io.netty.handler.codec.socksx.v5.Socks5InitRequest;
import io.netty.handler.codec.socksx.v5.Socks5InitResponse;
import io.netty.handler.codec.socksx.v5.Socks5InitResponseDecoder;
import io.netty.handler.codec.socksx.v5.Socks5MessageEncoder;
import io.netty.util.NetUtil;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;

public final class Socks5ProxyHandler extends ProxyHandler {

    private static final String PROTOCOL = "socks5";
    private static final String AUTH_PASSWORD = "password";
    private static final String AUTH_NONE = "none";

    private final String username;
    private final String password;

    public Socks5ProxyHandler(SocketAddress proxyAddress) {
        this(proxyAddress, null, null);
    }

    public Socks5ProxyHandler(SocketAddress proxyAddress, String username, String password) {
        super(proxyAddress);
        if (username != null && username.length() == 0) {
            username = null;
        }
        if (password != null && password.length() == 0) {
            password = null;
        }
        this.username = username;
        this.password = password;
    }

    @Override
    public String protocol() {
        return PROTOCOL;
    }

    @Override
    public String authScheme() {
        return socksAuthScheme() == Socks5AuthScheme.AUTH_PASSWORD? AUTH_PASSWORD : AUTH_NONE;
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
        p.addBefore(ctx.name(), "socks5decoder", new Socks5InitResponseDecoder());
        p.addBefore(ctx.name(), "socks5encoder", Socks5MessageEncoder.INSTANCE);
    }

    @Override
    protected void deconfigurePipeline(ChannelHandlerContext ctx) throws Exception {
        ctx.pipeline().remove("socks5encoder");
    }

    @Override
    protected Object newInitialMessage(ChannelHandlerContext ctx) throws Exception {
        return new Socks5InitRequest(Collections.singletonList(socksAuthScheme()));
    }

    @Override
    protected boolean handleResponse(ChannelHandlerContext ctx, Object response) throws Exception {
        if (response instanceof Socks5InitResponse) {
            Socks5InitResponse res = (Socks5InitResponse) response;
            Socks5AuthScheme authScheme = socksAuthScheme();

            if (authScheme != res.authScheme()) {
                // Server did not accept the requested authentication scheme.
                throw new ProxyConnectException(
                        "proxyAddress: " + proxyAddress() +
                        ", authScheme: " + authScheme + " (expected: " + res.authScheme() + ')');
            }

            if (authScheme == Socks5AuthScheme.AUTH_PASSWORD) {
                // In case of password authentication, send an authentication request.
                ctx.pipeline().addBefore("socks5encoder", "socks5decoder", new Socks5AuthResponseDecoder());
                sendToProxyServer(
                        new Socks5AuthRequest(username != null? username : "", password != null? password : ""));
            } else { // authScheme == NO_AUTH
                sendConnectCommand(ctx);
                return false;
            }
        }

        if (response instanceof Socks5AuthResponse) {
            // Received an authentication response from the server.
            Socks5AuthResponse res = (Socks5AuthResponse) response;
            if (res.authStatus() != Socks5AuthStatus.SUCCESS) {
                throw new ProxyConnectException(
                        "proxyAddress: " + proxyAddress() + ", authStatus: " + res.authStatus());
            }

            sendConnectCommand(ctx);
            return false;
        }

        // This should be the last message from the server.
        Socks5CmdResponse res = (Socks5CmdResponse) response;
        if (res.cmdStatus() != Socks5CmdStatus.SUCCESS) {
            throw new ProxyConnectException("proxyAddress: " + proxyAddress() + ", status: " + res.cmdStatus());
        }

        return true;
    }

    private Socks5AuthScheme socksAuthScheme() {
        Socks5AuthScheme authScheme;
        if (username == null && password == null) {
            authScheme = Socks5AuthScheme.NO_AUTH;
        } else {
            authScheme = Socks5AuthScheme.AUTH_PASSWORD;
        }
        return authScheme;
    }

    private void sendConnectCommand(ChannelHandlerContext ctx) throws Exception {
        InetSocketAddress raddr = destinationAddress();
        Socks5AddressType addrType;
        String rhost;
        if (raddr.isUnresolved()) {
            addrType = Socks5AddressType.DOMAIN;
            rhost = raddr.getHostString();
        } else {
            rhost = raddr.getAddress().getHostAddress();
            if (NetUtil.isValidIpV4Address(rhost)) {
                addrType = Socks5AddressType.IPv4;
            } else if (NetUtil.isValidIpV6Address(rhost)) {
                addrType = Socks5AddressType.IPv6;
            } else {
                throw new ProxyConnectException("unknown address type: " + rhost);
            }
        }

        ctx.pipeline().addBefore("socks5encoder", "socks5decoder", new Socks5CmdResponseDecoder());
        ctx.writeAndFlush(new Socks5CmdRequest(Socks5CmdType.CONNECT, addrType, rhost, raddr.getPort()));
    }
}
