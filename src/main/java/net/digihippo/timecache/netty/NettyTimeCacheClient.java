package net.digihippo.timecache.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import net.digihippo.timecache.TimeCacheActions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class NettyTimeCacheClient
{
    public static TimeCacheActions connect(
        String serverHost,
        int serverPort)
        throws InterruptedException
    {
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        final CompletableFuture<TimeCacheActions> future = new CompletableFuture<>();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                        new TimeCacheClientHandler(future));
                }
            });

            b.connect(serverHost, serverPort).sync();
            return future.get();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class TimeCacheClientHandler
        extends ChannelInboundHandlerAdapter
    {
        private final CompletableFuture<TimeCacheActions> future;
        private RemoteNettyTimeCacheActions actions;

        public TimeCacheClientHandler(CompletableFuture<TimeCacheActions> future)
        {
            this.future = future;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception
        {
            this.actions = new RemoteNettyTimeCacheActions(new NettyChannel(ctx));
            future.complete(actions);
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            ByteBuf message = (ByteBuf) msg;
            actions.dispatch(message);

            super.channelRead(ctx, msg);
        }
    }
}
