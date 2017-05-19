package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import net.digihippo.timecache.TimeCacheAgent;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

class AgentInvoker
{
    private final TimeCacheAgent timeCacheAgent;
    private final MessageReader messageReader = new MessageReader();

    AgentInvoker(TimeCacheAgent timeCacheAgent)
    {
        this.timeCacheAgent = timeCacheAgent;
    }

    void dispatch(ByteBuf message)
    {
        messageReader.readFrom(message);
        dispatchBuffer();
    }

    private void dispatchBuffer()
    {
        messageReader.flip();
        try
        {
            while (messageReader.hasBytes())
            {
                messageReader.mark();
                byte methodIndex = messageReader.readByte();
                switch (methodIndex)
                {
                    case 0:
                    {
                        String className = messageReader.readString();
                        timeCacheAgent.installDefinitions(className);
                        break;
                    }
                    case 1:
                    {
                        String cacheName = messageReader.readString();
                        long currentBucketStart = messageReader.readLong();
                        long currentBucketEnd = messageReader.readLong();
                        timeCacheAgent.populateBucket(cacheName, currentBucketStart, currentBucketEnd);
                        break;
                    }
                    case 2:
                    {
                        String cacheName = messageReader.readString();
                        long iterationKey = messageReader.readLong();
                        long from = messageReader.readLong();
                        long to = messageReader.readLong();
                        String installingClass = messageReader.readString();
                        String definitionName = messageReader.readString();
                        Optional<ByteBuffer> filterArgs = messageReader.readOptionalByteBuffer();

                        timeCacheAgent.iterate(
                            cacheName,
                            iterationKey,
                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(from), ZoneId.of("UTC")),
                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(to), ZoneId.of("UTC")),
                            installingClass,
                            definitionName,
                            filterArgs);
                        break;
                    }
                    case 3:
                    {
                        String cacheName = messageReader.readString();
                        String cacheComponentFactoryClass = messageReader.readString();
                        timeCacheAgent.defineCache(cacheName, cacheComponentFactoryClass);
                        break;
                    }
                    default:
                        throw new RuntimeException("Unknown method requested, index " + methodIndex);
                }
            }
        }
        catch (MessageReader.EndOfMessages endOfMessages)
        {
            messageReader.incompleteRead();
        }
        finally
        {
            messageReader.readComplete();
        }
    }
}
