package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import net.digihippo.timecache.DefinitionListener;
import net.digihippo.timecache.InstallationListener;
import net.digihippo.timecache.IterationListener;
import net.digihippo.timecache.TimeCacheAdministration;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

public class ClientToServerInvoker
{
    private final MessageReader messageReader = new MessageReader();

    private final TimeCacheAdministration timeCacheAdministration;

    public ClientToServerInvoker(TimeCacheAdministration timeCacheAdministration)
    {
        this.timeCacheAdministration = timeCacheAdministration;
    }

    public void dispatch(ChannelHandlerContext ctx, ByteBuf msg)
    {
        messageReader.readFrom(msg);
        messageReader.dispatchMessages(new MessageReader.Invoker()
        {
            @Override
            public void invokeOne(MessageReader messageReader) throws MessageReader.EndOfMessages
            {
                byte methodIndex = messageReader.readByte();
                switch (methodIndex)
                {
                    case 0:
                    {
                        long correlationId = messageReader.readLong();
                        String definitionName = messageReader.readString();
                        timeCacheAdministration.installDefinitions(
                            definitionName,
                            new InstallationListener(
                                () -> writeSuccessResponse(ctx, correlationId),
                                (errs) -> writeErrorResponse(ctx, correlationId, errs.toString())
                            ));
                    }
                    case 1:
                    {
                        long correlationId = messageReader.readLong();
                        String cacheName = messageReader.readString();
                        ZonedDateTime from = readUtc(messageReader);
                        ZonedDateTime to = readUtc(messageReader);
                        String definingClass = messageReader.readString();
                        String iterateeName = messageReader.readString();
                        Optional<ByteBuffer> byteBuffer = messageReader.readOptionalByteBuffer();
                        timeCacheAdministration.iterate(
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
                        long correlationId = messageReader.readLong();
                        String definitionsName = messageReader.readString();
                        String cacheComponentsFactoryClass = messageReader.readString();
                        timeCacheAdministration.defineCache(
                            definitionsName,
                            cacheComponentsFactoryClass,
                            new DefinitionListener(
                                (e) -> writeErrorResponse(ctx, correlationId, e),
                                () -> writeSuccessResponse(ctx, correlationId)));
                        break;
                    }
                    default:
                        throw new RuntimeException("Cannot invoke method of index " + methodIndex);
                }
            }
        });
    }

    private ZonedDateTime readUtc(MessageReader messageReader) throws MessageReader.EndOfMessages
    {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(messageReader.readLong()), ZoneId.of("UTC"));
    }

    private void writeSuccessResponse(
        ChannelHandlerContext ctx,
        long correlationId,
        ByteBuffer byteBuffer)
    {
        ByteBuf buffer = ctx.alloc().buffer(8 + 4 + 4 + byteBuffer.remaining());
        buffer.writeLong(correlationId);
        buffer.writeInt(1);
        buffer.writeInt(byteBuffer.remaining());
        buffer.writeBytes(byteBuffer);

        ctx.writeAndFlush(buffer);
    }

    private void writeSuccessResponse(
        ChannelHandlerContext ctx,
        long correlationId)
    {
        ByteBuf buffer = ctx.alloc().buffer(8 + 4 + 4);
        buffer.writeLong(correlationId);
        buffer.writeInt(1);
        buffer.writeInt(0);

        ctx.writeAndFlush(buffer);
    }

    private void writeErrorResponse(
        ChannelHandlerContext ctx,
        long correlationId,
        String message)
    {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        ByteBuf buffer = ctx.alloc().buffer(4 + 8 + bytes.length);
        buffer.writeLong(correlationId);
        buffer.writeInt(0);
        buffer.writeBytes(bytes);

        ctx.writeAndFlush(buffer);
    }
}
