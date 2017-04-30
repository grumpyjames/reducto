package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class NettyChannel implements Channel
{
    private final ChannelHandlerContext chc;

    public NettyChannel(ChannelHandlerContext chc)
    {
        this.chc = chc;
    }

    @Override
    public void write(ByteBuf buffer)
    {
        chc.writeAndFlush(buffer);
    }

    @Override
    public ByteBuf alloc(int messageLength)
    {
        return chc.alloc().buffer(messageLength);
    }
}