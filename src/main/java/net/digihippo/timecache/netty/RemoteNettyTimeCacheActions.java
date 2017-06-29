package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import net.digihippo.timecache.*;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

class RemoteNettyTimeCacheActions implements TimeCacheActions
{
    private final AtomicLong correlationIds = new AtomicLong(0);
    private final ConcurrentMap<Long, MessageReader.Invoker> inflight = new ConcurrentHashMap<>();

    private final Channel nettyChannel;
    private final MessageReader messageReader = new MessageReader();

    RemoteNettyTimeCacheActions(Channel nettyChannel)
    {
        this.nettyChannel = nettyChannel;
    }

    public void dispatch(ByteBuf byteBuf)
    {
        messageReader.dispatch(byteBuf, this::invokeOne);
    }

    private void invokeOne(MessageReader.Reader reader) throws MessageReader.EndOfMessages
    {
        long correlationId = reader.readLong();
        inflight.get(correlationId).invokeOne(reader);
    }

    @Override
    public void installDefinitions(
        String name,
        InstallationListener installationListener)
    {
        long correlationId = correlationIds.incrementAndGet();
        inflight.put(correlationId, reader -> {
            int success = reader.readInt();
            if (success == 1)
            {
                installationListener.onComplete.run();
            }
            else
            {
                String errorMessage = reader.readString();
                installationListener.onError.accept(errorMessage);
            }
            inflight.remove(correlationId);
        });

        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        ByteBuf byteBuf = nettyChannel.alloc(1 + 8 + 4 + bytes.length);
        byteBuf.writeByte(0);
        byteBuf.writeLong(correlationId);
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);

        nettyChannel.write(byteBuf);
    }

    @Override
    public void iterate(
        String cacheName,
        ZonedDateTime from,
        ZonedDateTime toExclusive,
        String definingClass,
        String iterateeName,
        Optional<ByteBuffer> filterArguments,
        IterationListener iterationListener)
    {
        long correlationId = correlationIds.incrementAndGet();
        inflight.put(correlationId, reader -> {
            int success = reader.readInt();
            if (success == 1)
            {
                int resultSize = reader.readInt();
                ByteBuffer byteBuffer = ByteBuffer.allocate(resultSize);
                reader.readBytes(resultSize, byteBuffer);
                byteBuffer.flip();
                iterationListener.onComplete.accept(new ReadableByteBuffer(byteBuffer));
            }
            else
            {
                iterationListener.onFatalError.accept(reader.readString());
            }

            inflight.remove(correlationId);
        });

        byte[] cacheNameBytes = cacheName.getBytes(StandardCharsets.UTF_8);
        long fromMillis = from.toInstant().toEpochMilli();
        long toMillis = toExclusive.toInstant().toEpochMilli();
        byte[] definingClassBytes = definingClass.getBytes(StandardCharsets.UTF_8);
        byte[] iterateeNameBytes = iterateeName.getBytes(StandardCharsets.UTF_8);
        int buflen = filterArguments.map(Buffer::remaining).orElse(0);

        final ByteBuf bb = nettyChannel.alloc(
            1 +
            4 + cacheNameBytes.length +
            8 + 8 +
            4 + definingClassBytes.length +
            4 + iterateeNameBytes.length +
            4 + buflen);

        bb.writeByte(1);
        bb.writeLong(correlationId);
        bb.writeInt(cacheNameBytes.length);
        bb.writeBytes(cacheNameBytes);
        bb.writeLong(fromMillis);
        bb.writeLong(toMillis);
        bb.writeInt(definingClassBytes.length);
        bb.writeBytes(definingClassBytes);
        bb.writeInt(iterateeNameBytes.length);
        bb.writeBytes(iterateeNameBytes);
        bb.writeInt(buflen);
        filterArguments.ifPresent(bb::writeBytes);

        nettyChannel.write(bb);
    }

    @Override
    public void defineCache(
        String cacheName,
        String cacheComponentFactoryClass,
        DefinitionListener definitionListener)
    {
        long correlationId = correlationIds.incrementAndGet();
        inflight.put(correlationId, messageReader1 -> {
            int success = messageReader1.readInt();
            if (success == 1)
            {
                definitionListener.onSuccess.run();
            }
            else
            {
                definitionListener.onError.accept(messageReader1.readString());
            }

            inflight.remove(correlationId);
        });


        byte[] cacheNameBytes = cacheName.getBytes(StandardCharsets.UTF_8);
        byte[] cacheComponentFactoryClassBytes = cacheComponentFactoryClass.getBytes(StandardCharsets.UTF_8);
        final ByteBuf byteBuf =
            nettyChannel.alloc(1 + 4 + cacheNameBytes.length + 4 + cacheComponentFactoryClassBytes.length);
        byteBuf.writeByte(2);
        byteBuf.writeLong(correlationId);
        byteBuf.writeInt(cacheNameBytes.length);
        byteBuf.writeBytes(cacheNameBytes);
        byteBuf.writeInt(cacheComponentFactoryClassBytes.length);
        byteBuf.writeBytes(cacheComponentFactoryClassBytes);

        nettyChannel.write(byteBuf);
    }

    @Override
    public void load(
        String cacheName,
        ZonedDateTime fromInclusive,
        ZonedDateTime toExclusive,
        LoadListener loadListener)
    {
        long correlationId = correlationIds.incrementAndGet();
        inflight.put(correlationId, reader -> {
            int success = reader.readInt();
            if (success == 1)
            {
                loadListener.onComplete.run();
            }
            else
            {
                loadListener.onFatalError.accept(reader.readString());
            }
            inflight.remove(correlationId);
        });

        byte[] cacheNameBytes = cacheName.getBytes(StandardCharsets.UTF_8);
        ByteBuf bb = nettyChannel.alloc(1 + 8 + 4 + cacheNameBytes.length + 8 + 8);
        bb.writeByte(3);
        bb.writeLong(correlationId);
        bb.writeInt(cacheNameBytes.length);
        bb.writeBytes(cacheNameBytes);
        bb.writeLong(fromInclusive.toInstant().toEpochMilli());
        bb.writeLong(toExclusive.toInstant().toEpochMilli());

        nettyChannel.write(bb);
    }
}
