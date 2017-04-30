package net.digihippo.timecache.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import net.digihippo.timecache.InMemoryTimeCacheAgent;
import net.digihippo.timecache.TimeCacheAgent;
import net.digihippo.timecache.TimeCacheServer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class NettyTimeCacheAgent
{
    public static void main(String[] args) throws InterruptedException
    {
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        try {
            Bootstrap b = new Bootstrap(); // (1)
            b.group(workerGroup); // (2)
            b.channel(NioSocketChannel.class); // (3)
            b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                        new TimeCacheAgentHandler());
                }
            });

            // Start the client.
            ChannelFuture f = b.connect("localhost", 8000).sync(); // (5)

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    private static final class EndOfMessages extends Exception
    {

    }

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final EndOfMessages END_OF_MESSAGES = new EndOfMessages();

    private static class TimeCacheAgentHandler
        extends ChannelInboundHandlerAdapter
        implements TimeCacheServer
    {
        private final ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        private TimeCacheAgent timeCacheAgent;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception
        {
            super.channelActive(ctx);
            ctx.alloc().buffer(1024);
            this.timeCacheAgent = new InMemoryTimeCacheAgent("moo", this);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            ByteBuf message = (ByteBuf) msg;
            int available = message.readableBytes();
            if (available > byteBuffer.remaining())
            {
                throw new RuntimeException("We just can't handle this packet length right now");
            }

            message.readBytes(byteBuffer.array(), byteBuffer.position(), available);
            byteBuffer.position(available);
            dispatchBuffer();

            super.channelRead(ctx, msg);
        }

        private void dispatchBuffer()
        {
            byteBuffer.flip();
            try
            {
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
                    }
                    case 3:
                    {
                        String cacheName = readString();
                        String cacheComponentFactoryClass = readString();
                        timeCacheAgent.defineCache(cacheName, cacheComponentFactoryClass);
                    }
                    default:
                        throw new RuntimeException("Unknown method requested, index " + methodIndex);
                }
            }
            catch (EndOfMessages eom)
            {
                // collapse any partial messages to the start of the buffer first, mind.
                byteBuffer.flip();
            }
        }

        @Override
        public void loadComplete(String agentId, String cacheName, long bucketStart, long bucketEnd)
        {

        }

        @Override
        public void bucketComplete(String agentId, String cacheName, long iterationKey, long currentBucketKey, Object result)
        {

        }

        @Override
        public void installationComplete(String agentName, String installationKlass)
        {

        }

        @Override
        public void installationError(String agentName, String installationKlass, String errorMessage)
        {

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

    }
}
