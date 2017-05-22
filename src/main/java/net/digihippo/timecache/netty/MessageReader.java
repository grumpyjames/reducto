package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

class MessageReader
{
    void dispatch(ByteBuf buffer, Invoker invokeOne)
    {
        innerReader.readFrom(buffer);
        dispatchMessages(invokeOne);
    }

    interface Invoker
    {
        void invokeOne(final Reader messageReader) throws EndOfMessages;
    }

    interface Reader
    {
        void readBytes(int length, ByteBuffer buffer) throws EndOfMessages;

        Optional<ByteBuffer> readOptionalByteBuffer() throws EndOfMessages;

        String readString() throws EndOfMessages;

        long readLong() throws EndOfMessages;

        int readInt() throws EndOfMessages;

        byte readByte() throws EndOfMessages;
    }

    private void dispatchMessages(Invoker invoker)
    {
        innerReader.flip();
        try
        {
            while (innerReader.hasBytes())
            {
                innerReader.mark();
                invoker.invokeOne(this.innerReader);
            }
        }
        catch (MessageReader.EndOfMessages endOfMessages)
        {
            innerReader.incompleteRead();
        }
        finally
        {
            innerReader.readComplete();
        }
    }

    private final InnerReader innerReader = new InnerReader();

    static final class EndOfMessages extends Exception {}

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final EndOfMessages END_OF_MESSAGES = new EndOfMessages();

    private static final class InnerReader implements Reader
    {
        @Override
        public void readBytes(int length, ByteBuffer buffer) throws EndOfMessages
        {
            checkAvailable(length);
            byteBuffer.get(buffer.array(), 0, length);
            buffer.position(length);
        }

        @Override
        public Optional<ByteBuffer> readOptionalByteBuffer() throws EndOfMessages
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

        @Override
        public String readString() throws EndOfMessages
        {
            final int length = readInt();
            checkAvailable(length);
            final byte[] contents = new byte[length];
            byteBuffer.get(contents, 0, length);
            return new String(contents, StandardCharsets.UTF_8);
        }

        @Override
        public long readLong() throws EndOfMessages
        {
            checkAvailable(8);
            return byteBuffer.getLong();
        }

        @Override
        public int readInt() throws EndOfMessages
        {
            checkAvailable(4);
            return byteBuffer.getInt();
        }

        @Override
        public byte readByte() throws EndOfMessages
        {
            checkAvailable(1);
            return byteBuffer.get();
        }

        private ByteBuffer byteBuffer = ByteBuffer.allocate(2048);

        private boolean hasBytes()
        {
            return byteBuffer.remaining() > 0;
        }

        private void incompleteRead()
        {
            byteBuffer.reset();
        }

        private void flip()
        {
            byteBuffer.flip();
        }

        private void mark()
        {
            byteBuffer.mark();
        }

        private void readComplete()
        {
            byteBuffer.compact();
        }

        private void readFrom(ByteBuf message)
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

        private void checkAvailable(int length) throws EndOfMessages
        {
            if (byteBuffer.remaining() < length)
            {
                throw END_OF_MESSAGES;
            }
        }
    }
}
