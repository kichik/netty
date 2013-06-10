/*
 * Copyright 2012 The Netty Project
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
package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.MessageList;
import io.netty.util.internal.TypeParameterMatcher;


/**
 * {@link ChannelOutboundHandlerAdapter} which encodes message in a stream-like fashion from one message to an
 * {@link ByteBuf}.
 *
 *
 * Example implementation which encodes {@link Integer}s to a {@link ByteBuf}.
 *
 * <pre>
 *     public class IntegerEncoder extends {@link MessageToByteEncoder}&lt;{@link Integer}&gt; {
 *         {@code @Override}
 *         public void encode({@link ChannelHandlerContext} ctx, {@link Integer} msg, {@link ByteBuf} out)
 *                 throws {@link Exception} {
 *             out.writeInt(msg);
 *         }
 *     }
 * </pre>
 */
public abstract class MessageToByteEncoder<I> extends ChannelOutboundHandlerAdapter {

    private final TypeParameterMatcher matcher;

    protected MessageToByteEncoder() {
        matcher = TypeParameterMatcher.find(this, MessageToByteEncoder.class, "I");
    }

    protected MessageToByteEncoder(Class<? extends I> outboundMessageType) {
        matcher = TypeParameterMatcher.get(outboundMessageType);
    }

    public boolean acceptOutboundMessage(Object msg) throws Exception {
        return matcher.match(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, MessageList<Object> msgs, ChannelPromise promise) throws Exception {
        MessageList<Object> out = MessageList.newInstance();
        boolean success = false;
        try {
            ByteBuf buf = null;
            int size = msgs.size();
            for (int i = 0; i < size; i ++) {
                Object m = msgs.get(i);
                if (!ctx.isRemoved() && acceptOutboundMessage(m)) {
                    @SuppressWarnings("unchecked")
                    I cast = (I) m;
                    if (buf == null) {
                        buf = ctx.alloc().buffer();
                    }
                    try {
                        encode(ctx, cast, buf);
                    } finally {
                        ByteBufUtil.release(cast);
                    }
                } else {
                    if (buf != null && buf.isReadable()) {
                        out.add(buf);
                        buf = null;
                    }

                    out.add(m);
                }
            }

            if (buf != null && buf.isReadable()) {
                out.add(buf);
            }

            success = true;
        } catch (EncoderException e) {
            throw e;
        } catch (Throwable e) {
            throw new EncoderException(e);
        } finally {
            msgs.recycle();
            if (success) {
                ctx.write(out, promise);
            } else {
                out.releaseAllAndRecycle();
            }
        }
    }

    /**
     * Encode a message into a {@link ByteBuf}. This method will be called till the {@link MessageList} has
     * nothing left.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param msg           the message to encode
     * @param out           the {@link ByteBuf} into which the encoded message will be written
     * @throws Exception    is thrown if an error accour
     */
    protected abstract void encode(ChannelHandlerContext ctx, I msg, ByteBuf out) throws Exception;
}