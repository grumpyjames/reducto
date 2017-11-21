package net.digihippo.timecache.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import net.digihippo.timecache.InMemoryTimeCacheAgent;

import java.io.File;
import java.net.InetSocketAddress;

public class NettyTimeCacheAgent
{
    public static void main(String[] args) throws InterruptedException
    {
        String agentName = args[0];
        final File file = new File(agentName);
        final boolean mkdir = file.mkdir();
        if (!mkdir)
        {
            throw new IllegalStateException("Unable to create agent data dir: " + file.getAbsolutePath());
        }

        connectAndRunAgent(file, "localhost", 8000);

    }

    public static void connectAndRunAgent(
        File rootDirectory,
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
                        new TimeCacheAgentHandler(rootDirectory));
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
    {
        private final File rootDir;
        private AgentInvoker agentInvoker;
        private RemoteNettyServer remoteServer;

        public TimeCacheAgentHandler(File rootDir)
        {
            this.rootDir = rootDir;
        }

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

            this.remoteServer = new RemoteNettyServer(new NettyChannel(ctx));
            this.agentInvoker =
                new AgentInvoker(
                    new InMemoryTimeCacheAgent(rootDir, host + ":" + port, remoteServer));
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            ByteBuf message = (ByteBuf) msg;
            agentInvoker.dispatch(message);

            super.channelRead(ctx, msg);
        }
    }
}
