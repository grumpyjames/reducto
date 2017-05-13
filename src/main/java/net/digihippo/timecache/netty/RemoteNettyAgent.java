package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import net.digihippo.timecache.TimeCacheAgent;

import java.time.ZonedDateTime;

import static net.digihippo.timecache.netty.Wire.*;

public class RemoteNettyAgent implements TimeCacheAgent
{
    private final Channel nettyChannel;

    public RemoteNettyAgent(Channel nettyChannel)
    {
        this.nettyChannel = nettyChannel;
    }

    @Override
    public void installDefinitions(String className)
    {
        byte[] utfEightBytes = utf8Bytes(className);
        int messageLength = 1 + wireLen(utfEightBytes);
        ByteBuf buffer = nettyChannel.alloc(messageLength);
        buffer.writeByte(0);
        writeBytes(buffer, utfEightBytes);
        nettyChannel.write(buffer);
    }

    @Override
    public void populateBucket(
        String cacheName,
        long currentBucketStart,
        long currentBucketEnd)
    {
        byte[] bytes = utf8Bytes(cacheName);
        ByteBuf buffer = nettyChannel.alloc(1 + wireLen(bytes) + 8 + 8);
        buffer.writeByte(1);
        writeBytes(buffer, bytes);
        buffer.writeLong(currentBucketStart);
        buffer.writeLong(currentBucketEnd);
        nettyChannel.write(buffer);
    }

    @Override
    public void iterate(
        String cacheName,
        long iterationKey,
        ZonedDateTime from,
        ZonedDateTime toExclusive,
        String installingClass,
        String definitionName)
    {
        final byte[] cacheNameBytes = utf8Bytes(cacheName);
        final byte[] installingClassBytes = utf8Bytes(installingClass);
        final byte[] definitionNameBytes = utf8Bytes(definitionName);
        ByteBuf buffer = nettyChannel.alloc(
            1 + wireLen(cacheNameBytes) + 8 + 8 + 8 +
                wireLen(installingClassBytes) +
                wireLen(definitionNameBytes));
        buffer.writeByte(2);
        writeBytes(buffer, cacheNameBytes);
        buffer.writeLong(iterationKey);
        buffer.writeLong(from.toInstant().toEpochMilli());
        buffer.writeLong(toExclusive.toInstant().toEpochMilli());
        writeBytes(buffer, installingClassBytes);
        writeBytes(buffer, definitionNameBytes);
        nettyChannel.write(buffer);
    }

    @Override
    public void defineCache(String cacheName, String cacheComponentFactoryClass)
    {
        byte[] cacheNameBytes = utf8Bytes(cacheName);
        byte[] cacheComponentFactoryClassBytes = utf8Bytes(cacheComponentFactoryClass);
        ByteBuf buffer =
            nettyChannel.alloc(
                1 + wireLen(cacheNameBytes) + wireLen(cacheComponentFactoryClassBytes));
        buffer.writeByte(3);
        writeBytes(buffer, cacheNameBytes);
        writeBytes(buffer, cacheComponentFactoryClassBytes);
        nettyChannel.write(buffer);
    }
}
