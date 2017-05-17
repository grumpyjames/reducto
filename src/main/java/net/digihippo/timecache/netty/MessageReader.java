package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MessageReader
{
    public void flip()
    {
        byteBuffer.flip();
    }

    public void mark()
    {
        byteBuffer.mark();
    }

    public void readComplete()
    {
        byteBuffer.compact();
    }

    public void readBytes(int length, ByteBuffer buffer) throws EndOfMessages
    {
        checkAvailable(length);
        byteBuffer.get(buffer.array(), 0, length);
        buffer.position(length);
    }

    public void incompleteRead()
    {
        byteBuffer.reset();
    }

    public boolean hasBytes()
    {
        return byteBuffer.remaining() > 0;
    }

    static final class EndOfMessages extends Exception {}

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final EndOfMessages END_OF_MESSAGES = new EndOfMessages();

    private final ByteBuffer byteBuffer = ByteBuffer.allocate(8192);

    void readFrom(ByteBuf message)
    {
        int available = message.readableBytes();
        if (available > byteBuffer.remaining())
        {
            throw new RuntimeException(
                "We just can't handle this packet length (" + available + " bytes) right now");
        }

        message.readBytes(byteBuffer.array(), byteBuffer.position(), available);
        byteBuffer.position(byteBuffer.position() + available);
    }


    String readString() throws EndOfMessages
    {
        final int length = readInt();
        checkAvailable(length);
        final byte[] contents = new byte[length];
        byteBuffer.get(contents, 0, length);
        return new String(contents, StandardCharsets.UTF_8);
    }

    long readLong() throws EndOfMessages
    {
        checkAvailable(8);
        return byteBuffer.getLong();
    }

    int readInt() throws EndOfMessages
    {
        checkAvailable(4);
        return byteBuffer.getInt();
    }

    byte readByte() throws EndOfMessages
    {
        checkAvailable(1);
        return byteBuffer.get();
    }

    private void checkAvailable(int length) throws EndOfMessages
    {
        if (byteBuffer.remaining() < length)
        {
            throw END_OF_MESSAGES;
        }
    }
}
