package net.digihippo.timecache.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import net.digihippo.timecache.InMemoryTimeCacheAgent;
import net.digihippo.timecache.TimeCacheServer;

import java.nio.ByteBuffer;

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

    private static class TimeCacheAgentHandler
        extends ChannelInboundHandlerAdapter
        implements TimeCacheServer
    {
        private AgentInvoker agentInvoker;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception
        {
            super.channelActive(ctx);
            this.agentInvoker = new AgentInvoker(new InMemoryTimeCacheAgent("moo", this));
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            ByteBuf message = (ByteBuf) msg;
            agentInvoker.dispatch(message);

            super.channelRead(ctx, msg);
        }

        @Override
        public void loadComplete(String agentId, String cacheName, long bucketStart, long bucketEnd)
        {

        }

        @Override
        public void bucketComplete(String agentId, String cacheName, long iterationKey, long currentBucketKey, ByteBuffer result)
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
    }
}
