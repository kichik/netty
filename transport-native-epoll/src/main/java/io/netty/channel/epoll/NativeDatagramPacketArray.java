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
package io.netty.channel.epoll;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.FastThreadLocal;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Support <a href="http://linux.die.net/man/2/sendmmsg">sendmmsg(...)</a> on linux with GLIBC 2.14+
 */
final class NativeDatagramPacketArray implements ChannelOutboundBuffer.MessageProcessor {

    private static final FastThreadLocal<NativeDatagramPacketArray> ARRAY =
            new FastThreadLocal<NativeDatagramPacketArray>() {
        @Override
        protected NativeDatagramPacketArray initialValue() throws Exception {
            return new NativeDatagramPacketArray();
        }
    };

    private final NativeDatagramPacket[] packets = new NativeDatagramPacket[Native.UIO_MAX_IOV];
    private int count;

    private NativeDatagramPacketArray() {
        for (int i = 0; i < packets.length; i++) {
            packets[i] = new NativeDatagramPacket();
        }
    }

    private boolean add(DatagramPacket packet) {
        if (count == packets.length) {
            return false;
        }
        ByteBuf content = packet.content();
        int len = content.readableBytes();
        if (len == 0) {
            return true;
        }
        NativeDatagramPacket p = packets[count];
        InetSocketAddress recipient = packet.recipient();
        InetAddress address = recipient.getAddress();
        if (address instanceof Inet6Address) {
            p.addr = address.getAddress();
            p.scopeId = ((Inet6Address) address).getScopeId();
        } else {
            p.addr = Native.ipv4MappedIpv6Address(address.getAddress());
            p.scopeId = 0;
        }
        p.port = recipient.getPort();
        p.memoryAddress = content.memoryAddress() + content.readerIndex();
        p.len = len;
        count++;
        return true;
    }

    @Override
    public boolean processMessage(Object msg) throws Exception {
        return msg instanceof DatagramPacket && add((DatagramPacket) msg);
    }

    /**
     * Returns the count
     */
    int count() {
        return count;
    }

    /**
     * Returns an array with {@link #count()}  {@link NativeDatagramPacket}s filled.
     */
    NativeDatagramPacket[] packets() {
        return packets;
    }

    /**
     * Returns a {@link NativeDatagramPacketArray} which is filled with the flushed messages of
     * {@link ChannelOutboundBuffer}.
     */
    static NativeDatagramPacketArray getInstance(ChannelOutboundBuffer buffer) throws Exception {
        NativeDatagramPacketArray array = ARRAY.get();
        array.count = 0;
        buffer.forEachFlushedMessage(array);
        return array;
    }

    /**
     * Used to pass needed data to JNI.
     */
    @SuppressWarnings("unused")
    static final class NativeDatagramPacket {
        private long memoryAddress;
        private int len;

        private byte[] addr;
        private int scopeId;
        private int port;
    }
}
