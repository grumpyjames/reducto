package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import net.digihippo.timecache.TimeCacheServer;

import java.nio.ByteBuffer;

public class ServerInvoker
{
    private final TimeCacheServer endpoint;

    private final MessageReader messageReader = new MessageReader();

    public ServerInvoker(TimeCacheServer endpoint)
    {
        this.endpoint = endpoint;
    }

    public void dispatch(ByteBuf buffer)
    {
        messageReader.dispatch(buffer, this::invokeOne);
    }

    private void invokeOne(MessageReader.Reader messageReader) throws MessageReader.EndOfMessages
    {
        byte methodIndex = messageReader.readByte();
        switch (methodIndex)
        {
            case 0:
            {
                String agentId = messageReader.readString();
                String cacheName = messageReader.readString();
                long bucketStart = messageReader.readLong();
                long bucketEnd = messageReader.readLong();
                endpoint.loadComplete(agentId, cacheName, bucketStart, bucketEnd);
                break;
            }
            case 1:
            {
                String agentId = messageReader.readString();
                String cacheName = messageReader.readString();
                long iterationKey = messageReader.readLong();
                long currentBucketKey = messageReader.readLong();
                int capacity = messageReader.readInt();
                ByteBuffer buffer = ByteBuffer.allocate(capacity);
                messageReader.readBytes(capacity, buffer);
                buffer.flip();

                endpoint.bucketComplete(agentId, cacheName, iterationKey, currentBucketKey, buffer);
                break;
            }
            case 2:
            {
                String agentId = messageReader.readString();
                String installationKlass = messageReader.readString();
                endpoint.installationComplete(agentId, installationKlass);
                break;
            }
            case 3:
            {
                String agentId = messageReader.readString();
                String installationKlass = messageReader.readString();
                String errorMessage = messageReader.readString();
                endpoint.installationError(agentId, installationKlass, errorMessage);
                break;
            }
            case 4:
            {
                String agentId = messageReader.readString();
                String cacheName = messageReader.readString();
                endpoint.cacheDefined(agentId, cacheName);
                break;
            }
            default:
                throw new RuntimeException("Unknown method requested, index " + methodIndex);
        }
    }
}
