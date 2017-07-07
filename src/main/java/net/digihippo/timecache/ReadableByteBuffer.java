package net.digihippo.timecache;

import net.digihippo.timecache.api.ReadBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ReadableByteBuffer implements ReadBuffer
{
    private final ByteBuffer bb;

    public ReadableByteBuffer(ByteBuffer bb)
    {
        this.bb = bb;
    }

    @Override
    public boolean readBoolean()
    {
        byte b = bb.get();
        return b != 0;
    }

    @Override
    public byte readByte()
    {
        return bb.get();
    }

    @Override
    public byte[] readBytes(int length)
    {
        byte[] result = new byte[length];
        bb.get(result);

        return result;
    }

    @Override
    public double readDouble()
    {
        return bb.getDouble();
    }

    @Override
    public float readFloat()
    {
        return bb.getFloat();
    }

    @Override
    public int readInt()
    {
        return bb.getInt();
    }

    @Override
    public long readLong()
    {
        return bb.getLong();
    }

    @Override
    public String readString()
    {
        int length = bb.getInt();
        byte[] bytes = readBytes(length);

        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public long size()
    {
        return bb.limit();
    }

    @Override
    public boolean hasBytes()
    {
        return bb.position() < bb.limit();
    }
}
