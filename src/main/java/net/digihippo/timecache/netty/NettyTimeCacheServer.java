package net.digihippo.timecache.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.digihippo.timecache.TimeCache;
import net.digihippo.timecache.TimeCacheEvents;

import java.net.InetSocketAddress;

public class NettyTimeCacheServer
{
    public static void main(String[] args) throws InterruptedException
    {
        startTimeCacheServer(8000, TimeCacheEvents.NO_OP);
    }

    public static TimeCache startTimeCacheServer(int port, TimeCacheEvents timeCacheEvents)
        throws InterruptedException
    {
        TimeCache timeCache = new TimeCache(timeCacheEvents);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1); // (1)
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);

        ServerBootstrap b = new ServerBootstrap(); // (2)
        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class) // (3)
            .childHandler(new ChannelInitializer<SocketChannel>()
            { // (4)
                @Override
                public void initChannel(SocketChannel ch) throws Exception
                {
                    ch.pipeline().addLast(new TimecacheServerHandler(timeCache));
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)          // (5)
            .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

        // Bind and start to accept incoming connections.
        ChannelFuture f = b.bind(port).sync(); // (7)
        timeCache.addShutdownHook(() -> {
            f.channel().close();
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        });

        return timeCache;
    }

    private static class TimecacheServerHandler extends ChannelInboundHandlerAdapter
    {
        private final TimeCache timeCache;
        private final ServerInvoker invoker;

        public TimecacheServerHandler(
            TimeCache timeCache)
        {
            this.invoker = new ServerInvoker(timeCache);
            this.timeCache = timeCache;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception
        {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            String host =
                inetSocketAddress
                    .getAddress()
                    .getHostAddress();
            int port = inetSocketAddress.getPort();
            RemoteNettyAgent cacheAgent = new RemoteNettyAgent(new NettyChannel(ctx));
            timeCache.addAgent(host + ":" + port, cacheAgent);

            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            invoker.dispatch((ByteBuf) msg);
            super.channelRead(ctx, msg);
        }
    }
}
