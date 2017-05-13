package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

final class Wire
{
    public static int wireLen(byte[] bytes)
    {
        return 4 + bytes.length;
    }

    public static byte[] utf8Bytes(String str)
    {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static void writeBytes(ByteBuf buffer, byte[] bytes)
    {
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
    }

    private Wire() {}
}
