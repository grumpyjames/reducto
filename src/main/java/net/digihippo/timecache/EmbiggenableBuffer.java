package net.digihippo.timecache;

import net.digihippo.timecache.api.WriteBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class EmbiggenableBuffer implements WriteBuffer
{
    private ByteBuffer byteBuffer;

    static EmbiggenableBuffer allocate(int baseSize)
    {
        return new EmbiggenableBuffer(ByteBuffer.allocate(baseSize));
    }

    private EmbiggenableBuffer(ByteBuffer byteBuffer)
    {
        this.byteBuffer = byteBuffer;
    }

    private void embiggenToFit(int extraBytes)
    {
        if (byteBuffer.remaining() < extraBytes)
        {
            ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.capacity() * 2);
            byteBuffer.flip();
            newBuffer.put(byteBuffer);
            byteBuffer = newBuffer;
        }
    }

    @Override
    public void putBoolean(boolean b)
    {
        embiggenToFit(1);
        byteBuffer.put(b ? (byte) 1 : (byte) 0);
    }

    @Override
    public void putByte(byte b)
    {
        embiggenToFit(1);
        byteBuffer.put(b);
    }

    @Override
    public void putBytes(byte[] bytes)
    {
        embiggenToFit(bytes.length);
        byteBuffer.put(bytes);
    }

    @Override
    public void putBytes(byte[] bytes, int offset, int length)
    {
        embiggenToFit(length);
        byteBuffer.put(bytes, offset, length);
    }

    @Override
    public void putDouble(double d)
    {
        embiggenToFit(8);
        byteBuffer.putDouble(d);
    }

    @Override
    public void putFloat(float f)
    {
        embiggenToFit(4);
        byteBuffer.putFloat(f);
    }

    @Override
    public void putInt(int i)
    {
        embiggenToFit(4);
        byteBuffer.putInt(i);
    }

    @Override
    public void putLong(long l)
    {
        embiggenToFit(8);
        byteBuffer.putLong(l);
    }

    @Override
    public void putString(String s)
    {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        putInt(bytes.length);
        putBytes(bytes);
    }

    public ByteBuffer asReadableByteBuffer()
    {
        byteBuffer.flip();
        return byteBuffer;
    }
}
