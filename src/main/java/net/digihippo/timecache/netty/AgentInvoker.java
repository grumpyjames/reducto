package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import net.digihippo.timecache.TimeCacheAgent;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class AgentInvoker
{
    private static final class EndOfMessages extends Exception {}

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final EndOfMessages END_OF_MESSAGES = new EndOfMessages();

    private final TimeCacheAgent timeCacheAgent;
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

    public AgentInvoker(TimeCacheAgent timeCacheAgent)
    {
        this.timeCacheAgent = timeCacheAgent;
    }

    private String readString() throws EndOfMessages
    {
        final int length = readInt();
        final byte[] contents = new byte[length];
        byteBuffer.get(contents, 0, length);
        return new String(contents, StandardCharsets.UTF_8);
    }

    private long readLong() throws EndOfMessages
    {
        checkAvailable(8);
        return byteBuffer.getLong();
    }

    private int readInt() throws EndOfMessages
    {
        checkAvailable(4);
        return byteBuffer.getInt();
    }

    private byte readByte() throws EndOfMessages
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

    public void dispatch(ByteBuf message)
    {
        int available = message.readableBytes();
        if (available > byteBuffer.remaining())
        {
            throw new RuntimeException("We just can't handle this packet length right now");
        }

        message.readBytes(byteBuffer.array(), byteBuffer.position(), available);
        byteBuffer.position(available);
        dispatchBuffer();
    }

    private void dispatchBuffer()
    {
        byteBuffer.flip();
        try
        {
            byteBuffer.mark();
            byte methodIndex = readByte();
            switch (methodIndex)
            {
                case 0:
                {
                    String className = readString();
                    timeCacheAgent.installDefinitions(className);
                    break;
                }
                case 1:
                {
                    String cacheName = readString();
                    long currentBucketStart = readLong();
                    long currentBucketEnd = readLong();
                    timeCacheAgent.populateBucket(cacheName, currentBucketStart, currentBucketEnd);
                    break;
                }
                case 2:
                {
                    String cacheName = readString();
                    long iterationKey = readLong();
                    long from = readLong();
                    long to = readLong();
                    String installingClass = readString();
                    String definitionName = readString();
                    timeCacheAgent.iterate(
                        cacheName,
                        iterationKey,
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(from), ZoneId.of("UTC")),
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(to), ZoneId.of("UTC")),
                        installingClass,
                        definitionName);
                    break;
                }
                case 3:
                {
                    String cacheName = readString();
                    String cacheComponentFactoryClass = readString();
                    timeCacheAgent.defineCache(cacheName, cacheComponentFactoryClass);
                    break;
                }
                default:
                    throw new RuntimeException("Unknown method requested, index " + methodIndex);
            }
        }
        catch (EndOfMessages eom)
        {
            byteBuffer.reset();
            // collapse any partial messages to the start of the buffer first, mind.
            byteBuffer.flip();
        }
    }
}
