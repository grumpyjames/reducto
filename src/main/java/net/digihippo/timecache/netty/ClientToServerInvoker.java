package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import net.digihippo.timecache.*;
import net.digihippo.timecache.api.ReadBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

class ClientToServerInvoker
{
    private final MessageReader messageReader = new MessageReader();

    private final TimeCacheActions timeCacheActions;

    ClientToServerInvoker(TimeCacheActions timeCacheActions)
    {
        this.timeCacheActions = timeCacheActions;
    }

    public void dispatch(ChannelHandlerContext ctx, ByteBuf msg)
    {
        messageReader.dispatch(msg, invokeOne(ctx));
    }

    private MessageReader.Invoker invokeOne(ChannelHandlerContext ctx)
    {
        return reader ->
        {
            byte methodIndex = reader.readByte();
            switch (methodIndex)
            {
                case 0:
                {
                    long correlationId = reader.readLong();
                    String definitionName = reader.readString();
                    timeCacheActions.installDefinitions(
                        definitionName,
                        new InstallationListener(
                            () -> writeSuccessResponse(ctx, correlationId),
                            (errs) -> writeErrorResponse(ctx, correlationId, errs)
                        ));
                    break;
                }
                case 1:
                {
                    long correlationId = reader.readLong();
                    String cacheName = reader.readString();
                    ZonedDateTime from = readUtc(reader);
                    ZonedDateTime to = readUtc(reader);
                    String definingClass = reader.readString();
                    String iterateeName = reader.readString();
                    Optional<ByteBuffer> byteBuffer = reader.readOptionalByteBuffer();
                    timeCacheActions.iterate(
                        cacheName,
                        from,
                        to,
                        definingClass,
                        iterateeName,
                        byteBuffer,
                        new IterationListener(
                            (bb) -> writeSuccessResponse(ctx, correlationId, bb),
                            (err) -> writeErrorResponse(ctx, correlationId, err)));
                    break;
                }
                case 2:
                {
                    long correlationId = reader.readLong();
                    String definitionsName = reader.readString();
                    String cacheComponentsFactoryClass = reader.readString();
                    timeCacheActions.defineCache(
                        definitionsName,
                        cacheComponentsFactoryClass,
                        new DefinitionListener(
                            () -> writeSuccessResponse(ctx, correlationId), (e) -> writeErrorResponse(ctx, correlationId, e)
                        ));
                    break;
                }
                case 3:
                {
                    long correlationId = reader.readLong();
                    String cacheName = reader.readString();
                    ZonedDateTime from = readUtc(reader);
                    ZonedDateTime to = readUtc(reader);

                    timeCacheActions.load(
                        cacheName,
                        from,
                        to,
                        new LoadListener(
                            () -> writeSuccessResponse(ctx, correlationId),
                            s -> writeErrorResponse(ctx, correlationId, s)
                        ));
                    break;
                }
                default:
                    throw new RuntimeException("Cannot invoke method of index " + methodIndex);
            }
        };
    }

    private ZonedDateTime readUtc(MessageReader.Reader reader) throws MessageReader.EndOfMessages
    {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(reader.readLong()), ZoneId.of("UTC"));
    }

    private void writeSuccessResponse(
        ChannelHandlerContext ctx,
        long correlationId,
        ReadBuffer byteBuffer)
    {
        int size = (int) byteBuffer.size();
        ByteBuf buffer = ctx.alloc().buffer(8 + 4 + 4 + size);
        buffer.writeLong(correlationId);
        buffer.writeInt(1);
        buffer.writeInt(size);
        buffer.writeBytes(byteBuffer.readBytes(size));

        ctx.writeAndFlush(buffer);
    }

    private void writeSuccessResponse(
        ChannelHandlerContext ctx,
        long correlationId)
    {
        ByteBuf buffer = ctx.alloc().buffer(8 + 4);
        buffer.writeLong(correlationId);
        buffer.writeInt(1);

        ctx.writeAndFlush(buffer);
    }

    private void writeErrorResponse(
        ChannelHandlerContext ctx,
        long correlationId,
        String message)
    {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        ByteBuf buffer = ctx.alloc().buffer(4 + 8 + 4 + bytes.length);
        buffer.writeLong(correlationId);
        buffer.writeInt(0);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);

        ctx.writeAndFlush(buffer);
    }
}
