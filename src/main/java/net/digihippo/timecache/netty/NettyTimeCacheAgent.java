package net.digihippo.timecache.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import net.digihippo.timecache.InMemoryTimeCacheAgent;
import net.digihippo.timecache.TimeCacheServer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class NettyTimeCacheAgent
{
    public static void main(String[] args) throws InterruptedException
    {
        connectAndRunAgent("localhost", 8000);
    }

    public static void connectAndRunAgent(
        String serverHost,
        int serverPort)
        throws InterruptedException
    {
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                        new TimeCacheAgentHandler());
                }
            });

            ChannelFuture f = b.connect(serverHost, serverPort).sync();

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
        private RemoteNettyServer remoteServer;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception
        {
            super.channelActive(ctx);
            InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().localAddress();
            String host =
                inetSocketAddress
                    .getAddress()
                    .getHostAddress();
            int port = inetSocketAddress.getPort();
            this.agentInvoker = new AgentInvoker(new InMemoryTimeCacheAgent(host + ":" + port, this));
            this.remoteServer = new RemoteNettyServer(new NettyChannel(ctx));
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
            remoteServer.loadComplete(agentId, cacheName, bucketStart, bucketEnd);
        }

        @Override
        public void bucketComplete(String agentId, String cacheName, long iterationKey, long currentBucketKey, ByteBuffer result)
        {
            remoteServer.bucketComplete(agentId, cacheName, iterationKey, currentBucketKey, result);
        }

        @Override
        public void installationComplete(String agentName, String installationKlass)
        {
            remoteServer.installationComplete(agentName, installationKlass);
        }

        @Override
        public void installationError(String agentName, String installationKlass, String errorMessage)
        {
            remoteServer.installationError(agentName, installationKlass, errorMessage);
        }

        @Override
        public void cacheDefined(String agentId, String cacheName)
        {
            remoteServer.cacheDefined(agentId, cacheName);
        }
    }
}
