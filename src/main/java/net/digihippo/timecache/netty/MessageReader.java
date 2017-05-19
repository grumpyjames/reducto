package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

class MessageReader
{
    void flip()
    {
        byteBuffer.flip();
    }

    void mark()
    {
        byteBuffer.mark();
    }

    void readComplete()
    {
        byteBuffer.compact();
    }

    void readBytes(int length, ByteBuffer buffer) throws EndOfMessages
    {
        checkAvailable(length);
        byteBuffer.get(buffer.array(), 0, length);
        buffer.position(length);
    }

    void incompleteRead()
    {
        byteBuffer.reset();
    }

    boolean hasBytes()
    {
        return byteBuffer.remaining() > 0;
    }

    Optional<ByteBuffer> readOptionalByteBuffer() throws EndOfMessages
    {
        final int length = readInt();
        if (length == 0)
        {
            return Optional.empty();
        }
        checkAvailable(length);
        final ByteBuffer result = ByteBuffer.allocate(length);
        result.put(byteBuffer.array(), byteBuffer.position(), length);
        result.position(length);
        result.flip();

        return Optional.of(result);
    }

    static final class EndOfMessages extends Exception {}

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final EndOfMessages END_OF_MESSAGES = new EndOfMessages();

    private ByteBuffer byteBuffer = ByteBuffer.allocate(2048);

    void readFrom(ByteBuf message)
    {
        int available = message.readableBytes();
        if (available > byteBuffer.remaining())
        {
            byteBuffer.flip();
            final ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.position() + available);
            newBuffer.put(byteBuffer);
            byteBuffer = newBuffer;
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
