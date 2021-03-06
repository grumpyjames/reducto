package net.digihippo.timecache.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.digihippo.timecache.TimeCache;
import net.digihippo.timecache.TimeCacheActions;
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

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>()
            {
                @Override
                public void initChannel(SocketChannel ch) throws Exception
                {
                    ch.pipeline().addLast(new TimecacheServerHandler(timeCache));
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        ChannelFuture serverToAgentFuture = b.bind(port).sync();

        ServerBootstrap clientToServer = new ServerBootstrap();
        clientToServer.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>()
            {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception
                {
                    ch.pipeline().addLast(new TimecacheClientServerHandler(timeCache));
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        ChannelFuture clientToServerFuture = clientToServer.bind(port + 1).sync();

        timeCache.addShutdownHook(() -> {
            serverToAgentFuture.channel().close();
            clientToServerFuture.channel().close();
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        });

        return timeCache;
    }

    private static final class TimecacheClientServerHandler
        extends ChannelInboundHandlerAdapter
    {
        private final TimeCacheActions timeCacheActions;
        private final ClientToServerInvoker invoker;

        public TimecacheClientServerHandler(TimeCacheActions timeCacheActions)
        {
            this.timeCacheActions = timeCacheActions;
            this.invoker = new ClientToServerInvoker(timeCacheActions);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception
        {
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            invoker.dispatch(ctx, (ByteBuf) msg);
            super.channelRead(ctx, msg);
        }
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
