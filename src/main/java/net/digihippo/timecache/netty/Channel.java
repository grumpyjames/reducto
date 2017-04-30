package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;

public interface Channel
{
    void write(ByteBuf buffer);

    ByteBuf alloc(int messageLength);
}
