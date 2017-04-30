package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import net.digihippo.timecache.TimeCacheAgent;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;

public class RemoteNettyAgent implements TimeCacheAgent
{
    private final ChannelHandlerContext chc;

    public RemoteNettyAgent(ChannelHandlerContext chc)
    {
        this.chc = chc;
    }

    @Override
    public void installDefinitions(String className)
    {
        byte[] utfEightBytes = utf8Bytes(className);
        ByteBuf buffer = chc.alloc().buffer(1 + wireLen(utfEightBytes));
        buffer.writeByte(0);
        writeBytes(utfEightBytes, buffer);
        chc.write(buffer);
        chc.flush();
    }

    @Override
    public void populateBucket(
        String cacheName,
        long currentBucketStart,
        long currentBucketEnd)
    {
        byte[] bytes = utf8Bytes(cacheName);
        ByteBuf buffer = chc.alloc().buffer(1 + wireLen(bytes) + 8 + 8);
        buffer.writeByte(1);
        writeBytes(bytes, buffer);
        buffer.writeLong(currentBucketStart);
        buffer.writeLong(currentBucketEnd);
        chc.write(buffer);
        chc.flush();
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
        ByteBuf buffer = chc.alloc().buffer(
                1 +
                wireLen(cacheNameBytes) +
                8 + 8 + 8 +
                wireLen(installingClassBytes) +
                wireLen(definitionNameBytes));
        buffer.writeByte(2);
        writeBytes(cacheNameBytes, buffer);
        buffer.writeLong(from.toInstant().toEpochMilli());
        buffer.writeLong(toExclusive.toInstant().toEpochMilli());
        writeBytes(installingClassBytes, buffer);
        writeBytes(definitionNameBytes, buffer);
        chc.write(buffer);
        chc.flush();
    }

    @Override
    public void defineCache(String cacheName, String cacheComponentFactoryClass)
    {
        byte[] cacheNameBytes = utf8Bytes(cacheName);
        byte[] bytes = utf8Bytes(cacheComponentFactoryClass);
        ByteBuf buffer = chc.alloc().buffer(1 + wireLen(cacheNameBytes) + wireLen(bytes));
        buffer.writeByte(3);
        writeBytes(cacheNameBytes, buffer);
        writeBytes(cacheNameBytes, buffer);
        chc.write(buffer);
        chc.flush();
    }

    private static int wireLen(byte[] cacheNameBytes)
    {
        return 4 + cacheNameBytes.length;
    }

    private static byte[] utf8Bytes(String cacheName)
    {
        return cacheName.getBytes(StandardCharsets.UTF_8);
    }

    private static void writeBytes(byte[] bytes, ByteBuf buffer)
    {
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
    }
}
