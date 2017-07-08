package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import net.digihippo.timecache.TimeCacheServer;

import java.nio.ByteBuffer;

import static net.digihippo.timecache.netty.Wire.*;

public class RemoteNettyServer implements TimeCacheServer
{
    private final Channel channel;

    public RemoteNettyServer(Channel channel)
    {
        this.channel = channel;
    }

    @Override
    public void loadComplete(
        String agentId,
        String cacheName,
        long bucketStart,
        long bucketEnd)
    {
        byte[] agentIdBytes = utf8Bytes(agentId);
        byte[] cacheNameBytes = utf8Bytes(cacheName);
        ByteBuf alloc = channel.alloc(
            1 +
            wireLen(agentIdBytes) +
            wireLen(cacheNameBytes) +
            8 +
            8);
        alloc.writeByte(0);
        writeBytes(alloc, agentIdBytes);
        writeBytes(alloc, cacheNameBytes);
        alloc.writeLong(bucketStart);
        alloc.writeLong(bucketEnd);

        channel.write(alloc);
    }

    @Override
    public void bucketComplete(
        String agentId,
        String cacheName,
        long iterationKey,
        long currentBucketKey,
        ByteBuffer buffer)
    {
        byte[] agentIdBytes = utf8Bytes(agentId);
        byte[] cacheNameBytes = utf8Bytes(cacheName);
        ByteBuf alloc = channel.alloc(
            1 +
            wireLen(agentIdBytes) +
            wireLen(cacheNameBytes) +
            8 +
            8 +
            4 + buffer.limit());
        alloc.writeByte(1);
        writeBytes(alloc, agentIdBytes);
        writeBytes(alloc, cacheNameBytes);
        alloc.writeLong(iterationKey);
        alloc.writeLong(currentBucketKey);

        alloc.writeInt(buffer.limit());
        alloc.writeBytes(buffer);

        channel.write(alloc);
    }

    @Override
    public void installationComplete(
        String agentName,
        String installationKlass)
    {
        byte[] agentIdBytes = utf8Bytes(agentName);
        byte[] installationKlassBytes = utf8Bytes(installationKlass);
        ByteBuf alloc = channel.alloc(
            1 +
            wireLen(agentIdBytes) +
            wireLen(installationKlassBytes));

        alloc.writeByte(2);
        writeBytes(alloc, agentIdBytes);
        writeBytes(alloc, installationKlassBytes);

        channel.write(alloc);
    }

    @Override
    public void installationError(
        String agentName,
        String installationKlass,
        String errorMessage)
    {
        byte[] agentIdBytes = utf8Bytes(agentName);
        byte[] installationKlassBytes = utf8Bytes(installationKlass);
        byte[] errorBytes = utf8Bytes(errorMessage);
        ByteBuf alloc = channel.alloc(
            1 +
                wireLen(agentIdBytes) +
                wireLen(installationKlassBytes) +
            wireLen(errorBytes));
        alloc.writeByte(3);
        writeBytes(alloc, agentIdBytes);
        writeBytes(alloc, installationKlassBytes);
        writeBytes(alloc, errorBytes);

        channel.write(alloc);
    }

    @Override
    public void cacheDefined(String agentId, String cacheName)
    {
        byte[] agentIdBytes = utf8Bytes(agentId);
        byte[] cacheNameBytes = utf8Bytes(cacheName);
        ByteBuf alloc = channel.alloc(1 + wireLen(agentIdBytes) + wireLen(cacheNameBytes));

        alloc.writeByte(4);
        writeBytes(alloc, agentIdBytes);
        writeBytes(alloc, cacheNameBytes);

        channel.write(alloc);
    }

    @Override
    public void loadFailure(String agentId, String cacheName, long currentBucketStart, String message)
    {
        byte[] agentIdBytes = utf8Bytes(agentId);
        byte[] cacheNameBytes = utf8Bytes(cacheName);
        byte[] messageBytes = utf8Bytes(message);

        ByteBuf alloc = channel.alloc(
            1 +
                wireLen(agentIdBytes) +
                wireLen(cacheNameBytes) +
                8 +
                wireLen(messageBytes));

        alloc.writeByte(5);
        writeBytes(alloc, agentIdBytes);
        writeBytes(alloc, cacheNameBytes);
        alloc.writeLong(currentBucketStart);
        writeBytes(alloc, messageBytes);

        channel.write(alloc);
    }


    @Override
    public void cacheDefinitionFailed(String agentId, String cacheName, String errorMessage)
    {
        byte[] agentIdBytes = utf8Bytes(agentId);
        byte[] cacheNameBytes = utf8Bytes(cacheName);
        byte[] messageBytes = utf8Bytes(errorMessage);

        ByteBuf alloc = channel.alloc(
            1 +
                wireLen(agentIdBytes) +
                wireLen(cacheNameBytes) +
                wireLen(messageBytes));

        alloc.writeByte(6);
        writeBytes(alloc, agentIdBytes);
        writeBytes(alloc, cacheNameBytes);
        writeBytes(alloc, messageBytes);

        channel.write(alloc);
    }
}
