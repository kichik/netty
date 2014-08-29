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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ProxyHandlerTest {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ProxyHandlerTest.class);

    private static final InetSocketAddress DESTINATION = InetSocketAddress.createUnresolved("destination.com", 42);
    private static final InetSocketAddress BAD_DESTINATION = new InetSocketAddress("1.2.3.4", 5);
    private static final String USERNAME = "testUser";
    private static final String PASSWORD = "testPassword";
    private static final String BAD_USERNAME = "badUser";
    private static final String BAD_PASSWORD = "badPassword";

    static final EventLoopGroup group = new NioEventLoopGroup(3, new DefaultThreadFactory("proxy", true));

    static final SslContext serverSslCtx;
    static final SslContext clientSslCtx;

    static {
        SslContext sctx;
        SslContext cctx;
        try {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sctx = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
            cctx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        } catch (Exception e) {
            throw new Error(e);
        }
        serverSslCtx = sctx;
        clientSslCtx = cctx;
    }

    static final ProxyServer deadHttpProxy = new HttpProxyServer(false, TestMode.UNRESPONSIVE, DESTINATION);
    static final ProxyServer anonHttpProxy = new HttpProxyServer(false, TestMode.TERMINAL, DESTINATION);
    static final ProxyServer httpProxy =
            new HttpProxyServer(false, TestMode.TERMINAL, DESTINATION, USERNAME, PASSWORD);

    static final ProxyServer deadHttpsProxy = new HttpProxyServer(true, TestMode.UNRESPONSIVE, DESTINATION);
    static final ProxyServer anonHttpsProxy = new HttpProxyServer(true, TestMode.TERMINAL, DESTINATION);
    static final ProxyServer httpsProxy =
            new HttpProxyServer(true, TestMode.TERMINAL, DESTINATION, USERNAME, PASSWORD);

    static final ProxyServer deadSocks4Proxy = new Socks4ProxyServer(false, TestMode.UNRESPONSIVE, DESTINATION);
    static final ProxyServer anonSocks4Proxy = new Socks4ProxyServer(false, TestMode.TERMINAL, DESTINATION);
    static final ProxyServer socks4Proxy = new Socks4ProxyServer(false, TestMode.TERMINAL, DESTINATION, USERNAME);

    static final ProxyServer deadSocks5Proxy = new Socks5ProxyServer(false, TestMode.UNRESPONSIVE, DESTINATION);
    static final ProxyServer anonSocks5Proxy = new Socks5ProxyServer(false, TestMode.TERMINAL, DESTINATION);
    static final ProxyServer socks5Proxy =
            new Socks5ProxyServer(false, TestMode.TERMINAL, DESTINATION, USERNAME, PASSWORD);

    @Parameters(name = "{index}: {0}")
    public static List<Object[]> testItems() {
        List<TestItem> items = new ArrayList<TestItem>();

        // HTTP --------------------------------------------------------------

        // Tests for anonymous HTTP proxy connections
        items.add(new SuccessTestItem(
                anonHttpProxy, DESTINATION,
                new HttpProxyHandler(anonHttpProxy.address())));

        items.add(new FailureTestItem(
                anonHttpProxy, BAD_DESTINATION, "status: 403",
                new HttpProxyHandler(anonHttpProxy.address())));

        items.add(new FailureTestItem(
                httpProxy, DESTINATION, "status: 401",
                new HttpProxyHandler(httpProxy.address())));

        // Tests for authenticated HTTP proxy connections
        items.add(new SuccessTestItem(
                httpProxy, DESTINATION,
                new HttpProxyHandler(httpProxy.address(), USERNAME, PASSWORD)));

        items.add(new FailureTestItem(
                httpProxy, BAD_DESTINATION, "status: 403",
                new HttpProxyHandler(httpProxy.address(), USERNAME, PASSWORD)));

        items.add(new FailureTestItem(
                httpProxy, DESTINATION, "status: 401",
                new HttpProxyHandler(httpProxy.address(), BAD_USERNAME, BAD_PASSWORD)));

        // Test for an unresponsive HTTP proxy connection.
        items.add(new TimeoutTestItem(
                deadHttpProxy, new HttpProxyHandler(deadHttpProxy.address())));

        // HTTPS --------------------------------------------------------------

        // Tests for anonymous HTTPS proxy connections
        items.add(new SuccessTestItem(
                anonHttpsProxy, DESTINATION,
                clientSslCtx.newHandler(PooledByteBufAllocator.DEFAULT),
                new HttpProxyHandler(anonHttpsProxy.address())));

        items.add(new FailureTestItem(
                anonHttpsProxy, BAD_DESTINATION, "status: 403",
                clientSslCtx.newHandler(PooledByteBufAllocator.DEFAULT),
                new HttpProxyHandler(anonHttpsProxy.address())));

        items.add(new FailureTestItem(
                httpsProxy, DESTINATION, "status: 401",
                clientSslCtx.newHandler(PooledByteBufAllocator.DEFAULT),
                new HttpProxyHandler(httpsProxy.address())));

        // Tests for authenticated HTTPS proxy connections
        items.add(new SuccessTestItem(
                httpsProxy, DESTINATION,
                clientSslCtx.newHandler(PooledByteBufAllocator.DEFAULT),
                new HttpProxyHandler(httpsProxy.address(), USERNAME, PASSWORD)));

        items.add(new FailureTestItem(
                httpsProxy, BAD_DESTINATION, "status: 403",
                clientSslCtx.newHandler(PooledByteBufAllocator.DEFAULT),
                new HttpProxyHandler(httpsProxy.address(), USERNAME, PASSWORD)));

        items.add(new FailureTestItem(
                httpsProxy, DESTINATION, "status: 401",
                clientSslCtx.newHandler(PooledByteBufAllocator.DEFAULT),
                new HttpProxyHandler(httpsProxy.address(), BAD_USERNAME, BAD_PASSWORD)));

        // Test for an unresponsive HTTPS proxy connection.
        items.add(new TimeoutTestItem(
                deadHttpsProxy,
                clientSslCtx.newHandler(PooledByteBufAllocator.DEFAULT),
                new HttpProxyHandler(deadHttpsProxy.address())));

        // SOCKS4 ------------------------------------------------------------

        // Tests for anonymous SOCKS4 proxy connections
        items.add(new SuccessTestItem(
                anonSocks4Proxy, DESTINATION,
                new Socks4ProxyHandler(anonSocks4Proxy.address())));

        items.add(new FailureTestItem(
                anonSocks4Proxy, BAD_DESTINATION, "REJECTED_OR_FAILED",
                new Socks4ProxyHandler(anonSocks4Proxy.address())));

        items.add(new FailureTestItem(
                socks4Proxy, DESTINATION, "IDENTD_AUTH_FAILURE",
                new Socks4ProxyHandler(socks4Proxy.address())));

        // Tests for authenticated HTTP proxy connections
        items.add(new SuccessTestItem(
                socks4Proxy, DESTINATION,
                new Socks4ProxyHandler(socks4Proxy.address(), USERNAME)));

        items.add(new FailureTestItem(
                socks4Proxy, BAD_DESTINATION, "REJECTED_OR_FAILED",
                new Socks4ProxyHandler(socks4Proxy.address(), USERNAME)));

        items.add(new FailureTestItem(
                socks4Proxy, DESTINATION, "IDENTD_AUTH_FAILURE",
                new Socks4ProxyHandler(socks4Proxy.address(), BAD_USERNAME)));

        // Test for an unresponsive HTTP proxy connection.
        items.add(new TimeoutTestItem(
                deadSocks4Proxy, new Socks4ProxyHandler(deadSocks4Proxy.address())));

        // SOCKS5 ------------------------------------------------------------

        // Tests for anonymous SOCKS5 proxy connections
        items.add(new SuccessTestItem(
                anonSocks5Proxy, DESTINATION,
                new Socks5ProxyHandler(anonSocks5Proxy.address())));

        items.add(new FailureTestItem(
                anonSocks5Proxy, BAD_DESTINATION, "cmdStatus: FORBIDDEN",
                new Socks5ProxyHandler(anonSocks5Proxy.address())));

        items.add(new FailureTestItem(
                socks5Proxy, DESTINATION, "unexpected authScheme: AUTH_PASSWORD",
                new Socks5ProxyHandler(socks5Proxy.address())));

        // Tests for authenticated SOCKS5 proxy connections
        items.add(new SuccessTestItem(
                socks5Proxy, DESTINATION,
                new Socks5ProxyHandler(socks5Proxy.address(), USERNAME, PASSWORD)));

        items.add(new FailureTestItem(
                socks5Proxy, BAD_DESTINATION, "cmdStatus: FORBIDDEN",
                new Socks5ProxyHandler(socks5Proxy.address(), USERNAME, PASSWORD)));

        items.add(new FailureTestItem(
                socks5Proxy, DESTINATION, "authStatus: FAILURE",
                new Socks5ProxyHandler(socks5Proxy.address(), BAD_USERNAME, BAD_PASSWORD)));

        // Test for an unresponsive SOCKS5 proxy connection.
        items.add(new TimeoutTestItem(
                deadSocks5Proxy, new Socks5ProxyHandler(deadSocks5Proxy.address())));

        // Convert the test items to the list of constructor parameters.
        List<Object[]> params = new ArrayList<Object[]>(items.size());
        for (TestItem i: items) {
            params.add(new Object[] { i });
        }

        return params;
    }

    @AfterClass
    public static void stopProxyServers() {
        // HTTP proxy servers
        httpProxy.stop();
        anonHttpProxy.stop();
        deadHttpProxy.stop();

        // HTTPS proxy servers
        httpsProxy.stop();
        anonHttpsProxy.stop();
        deadHttpsProxy.stop();

        // SOCKS4 proxy servers
        socks4Proxy.stop();
        anonSocks4Proxy.stop();
        deadSocks4Proxy.stop();

        // SOCKS5 proxy servers
        socks5Proxy.stop();
        anonSocks5Proxy.stop();
        deadSocks5Proxy.stop();
    }

    private final TestItem testItem;

    public ProxyHandlerTest(TestItem testItem) {
        this.testItem = testItem;
    }

    @Test
    public void test() throws Exception {
        testItem.test();
    }
    private static final class SuccessTestHandler extends SimpleChannelInboundHandler<Object> {

        final Queue<String> received = new LinkedBlockingQueue<String>();
        final Queue<Throwable> exceptions = new LinkedBlockingQueue<Throwable>();

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.writeAndFlush(Unpooled.copiedBuffer("A\n", CharsetUtil.US_ASCII));
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProxyConnectionEvent) {
                ctx.writeAndFlush(Unpooled.copiedBuffer("B\n", CharsetUtil.US_ASCII));
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            String str = ((ByteBuf) msg).toString(CharsetUtil.US_ASCII);
            received.add(str);
            if ("2".equals(str)) {
                ctx.writeAndFlush(Unpooled.copiedBuffer("C\n", CharsetUtil.US_ASCII));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            exceptions.add(cause);
            ctx.close();
        }
    }

    private static final class FailureTestHandler extends SimpleChannelInboundHandler<Object> {

        final Queue<Throwable> exceptions = new LinkedBlockingQueue<Throwable>();

        /**
         * A latch that counts down when:
         * - a pending write attempt in {@link #channelActive(ChannelHandlerContext)} finishes, or
         * - the channel is closed.
         * By waiting until the latch goes down to 0, we can make sure all assertion failures related with all write
         * attempts have been recorded.
         */
        final CountDownLatch latch = new CountDownLatch(2);

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.writeAndFlush(Unpooled.copiedBuffer("A\n", CharsetUtil.US_ASCII)).addListener(
                    new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            latch.countDown();
                            if (!(future.cause() instanceof ProxyConnectException)) {
                                exceptions.add(new AssertionError(
                                        "Unexpected failure cause for initial write: " + future.cause()));
                            }
                        }
                    });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            latch.countDown();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProxyConnectionEvent) {
                fail("Unexpected event: " + evt);
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            fail("Unexpected message: " + msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            exceptions.add(cause);
            ctx.close();
        }
    }

    private abstract static class TestItem {
        protected final List<ProxyServer> servers;
        protected final InetSocketAddress destination;
        protected final ChannelHandler[] clientHandlers;

        protected TestItem(ProxyServer server, InetSocketAddress destination, ChannelHandler... clientHandlers) {
            this(Collections.singletonList(server), destination, clientHandlers);
        }

        protected TestItem(
                List<ProxyServer> servers, InetSocketAddress destination, ChannelHandler... clientHandlers) {
            this.servers = servers;
            this.destination = destination;
            this.clientHandlers = clientHandlers;
        }

        final void test() throws Exception {
            for (ProxyServer s: servers) {
                s.clearExceptions();
            }

            doTest();

            for (ProxyServer s: servers) {
                s.checkExceptions();
            }
        }

        protected abstract void doTest() throws Exception;

        protected void assertProxyHandlers(boolean success) {
            for (ChannelHandler h: clientHandlers) {
                if (h instanceof ProxyHandler) {
                    ProxyHandler ph = (ProxyHandler) h;
                    String type = StringUtil.simpleClassName(ph);
                    Future<Channel> f = ph.connectFuture();
                    if (!f.isDone()) {
                        logger.warn("{}: not done", type);
                    } else if (f.isSuccess()) {
                        if (success) {
                            logger.debug("{}: success", type);
                        } else {
                            logger.warn("{}: success", type);
                        }
                    } else {
                        if (success) {
                            logger.warn("{}: failure", type, f.cause());
                        } else {
                            logger.debug("{}: failure", type, f.cause());
                        }
                    }
                }
            }

            for (ChannelHandler h: clientHandlers) {
                if (h instanceof ProxyHandler) {
                    ProxyHandler ph = (ProxyHandler) h;
                    assertThat(ph.connectFuture().isDone(), is(true));
                    assertThat(ph.connectFuture().isSuccess(), is(success));
                }
            }
        }
    }

    private static final class SuccessTestItem extends TestItem {

        SuccessTestItem(ProxyServer server, InetSocketAddress destination, ChannelHandler... clientHandlers) {
            super(server, destination, clientHandlers);
        }

        SuccessTestItem(List<ProxyServer> servers, InetSocketAddress destination, ChannelHandler... clientHandlers) {
            super(servers, destination, clientHandlers);
        }

        @Override
        protected void doTest() throws Exception {
            final SuccessTestHandler testHandler = new SuccessTestHandler();
            Bootstrap b = new Bootstrap();
            b.group(group);
            b.channel(NioSocketChannel.class);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(clientHandlers);
                    p.addLast(new LineBasedFrameDecoder(64));
                    p.addLast(testHandler);
                }
            });

            boolean finished = b.connect(destination).channel().closeFuture().await(10, TimeUnit.SECONDS);

            logger.debug("Received messages: {}", testHandler.received);

            if (testHandler.exceptions.isEmpty()) {
                logger.debug("No recorded exceptions on the client side.");
            } else {
                for (Throwable t : testHandler.exceptions) {
                    logger.debug("Recorded exception on the client side: {}", t);
                }
            }

            assertProxyHandlers(true);

            assertThat(testHandler.received.toArray(), is(new Object[] { "0", "1", "2", "3" }));
            assertThat(testHandler.exceptions.toArray(), is(EmptyArrays.EMPTY_OBJECTS));
            assertThat(finished, is(true));
        }
    }

    private static final class FailureTestItem extends TestItem {

        private final String expectedMessage;

        FailureTestItem(ProxyServer server, InetSocketAddress destination, String expectedMessage,
                        ChannelHandler... clientHandlers) {
            super(server, destination, clientHandlers);
            this.expectedMessage = expectedMessage;
        }

        FailureTestItem(List<ProxyServer> servers, InetSocketAddress destination, String expectedMessage,
                        ChannelHandler... clientHandlers) {
            super(servers, destination, clientHandlers);
            this.expectedMessage = expectedMessage;
        }

        @Override
        protected void doTest() throws Exception {
            final FailureTestHandler testHandler = new FailureTestHandler();
            Bootstrap b = new Bootstrap();
            b.group(group);
            b.channel(NioSocketChannel.class);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(clientHandlers);
                    p.addLast(new LineBasedFrameDecoder(64));
                    p.addLast(testHandler);
                }
            });

            boolean finished = b.connect(destination).channel().closeFuture().await(10, TimeUnit.SECONDS);
            finished &= testHandler.latch.await(10, TimeUnit.SECONDS);

            logger.debug("Recorded exceptions: {}", testHandler.exceptions);

            assertProxyHandlers(false);

            assertThat(testHandler.exceptions.size(), is(1));
            Throwable e = testHandler.exceptions.poll();
            assertThat(e, is(instanceOf(ProxyConnectException.class)));
            assertThat(String.valueOf(e), containsString(expectedMessage));
            assertThat(finished, is(true));
        }
    }

    private static final class TimeoutTestItem extends TestItem {

        TimeoutTestItem(ProxyServer server, ChannelHandler... clientHandlers) {
            super(server, null, clientHandlers);
        }

        TimeoutTestItem(List<ProxyServer> servers, ChannelHandler... clientHandlers) {
            super(servers, null, clientHandlers);
        }

        @Override
        protected void doTest() throws Exception {
            final long TIMEOUT = 2000;
            for (ChannelHandler h: clientHandlers) {
                if (h instanceof ProxyHandler) {
                    ((ProxyHandler) h).setConnectTimeoutMillis(TIMEOUT);
                }
            }

            final FailureTestHandler testHandler = new FailureTestHandler();
            Bootstrap b = new Bootstrap();
            b.group(group);
            b.channel(NioSocketChannel.class);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(clientHandlers);
                    p.addLast(new LineBasedFrameDecoder(64));
                    p.addLast(testHandler);
                }
            });

            ChannelFuture cf = b.connect(DESTINATION).channel().closeFuture();
            boolean finished = cf.await(TIMEOUT * 2, TimeUnit.MILLISECONDS);
            finished &= testHandler.latch.await(TIMEOUT * 2, TimeUnit.MILLISECONDS);

            logger.debug("Recorded exceptions: {}", testHandler.exceptions);

            assertProxyHandlers(false);

            assertThat(testHandler.exceptions.size(), is(1));
            Throwable e = testHandler.exceptions.poll();
            assertThat(e, is(instanceOf(ProxyConnectException.class)));
            assertThat(String.valueOf(e), containsString("timeout"));
            assertThat(finished, is(true));
        }
    }
}
