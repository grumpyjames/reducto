package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.jmock.Expectations;
import org.jmock.Mockery;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;

final class ThoroughSerializationTesting
{
    static <T> void runTest(
        final Mockery mockery,
        final Class<T> target,
        final Function<T, Consumer<ByteBuf>> invokerFactory,
        final Function<Channel, T> stubCreator,
        final List<Invocation<T>> possibleInvocations
    )
    {
        final Random random = new Random(System.currentTimeMillis());
        final T mock = mockery.mock(target, "thorough serialization target");
        final Consumer<ByteBuf> invoker = invokerFactory.apply(mock);

        runWith(mockery, stubCreator, possibleInvocations, random, mock, new RandomFragmentChannel(random, invoker));
        runWith(mockery, stubCreator, possibleInvocations, random, mock, new OneGiantMessageChannel(invoker));
    }

    interface FlushableChannel extends Channel
    {
        void flush();
    }

    private static <T> void runWith(
        final Mockery mockery,
        final Function<Channel, T> stubCreator,
        final List<Invocation<T>> possibleInvocations,
        final Random random,
        final T mock,
        final FlushableChannel channel)
    {
        final T stub = stubCreator.apply(channel);
        final List<Invocation<T>> invocations = generateList(
            random, r -> possibleInvocations.get(r.nextInt(possibleInvocations.size())));

        for (Invocation<T> invocation : invocations)
        {
            mockery.checking(
                new Expectations()
                {{
                    invocation.run(oneOf(mock));
                }}
            );
            invocation.run(stub);
        }

        channel.flush();
    }

    private static <T> List<T> generateList(
        final Random random, final Function<Random, T> generator)
    {
        int size = random.nextInt(2000);
        final List<T> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            result.add(generator.apply(random));
        }

        return result;
    }

    private ThoroughSerializationTesting() {}

    private static class RandomFragmentChannel implements Channel, FlushableChannel
    {
        private final ByteBufAllocator allocator;
        private final Random random;
        private final Consumer<ByteBuf> invoker;

        RandomFragmentChannel(Random random, Consumer<ByteBuf> invoker)
        {
            this.random = random;
            this.invoker = invoker;
            this.allocator = new UnpooledByteBufAllocator(false, true);
        }

        @Override
        public void write(ByteBuf buffer)
        {
            int delivered = 0;
            int toDeliver = buffer.readableBytes();
            while (delivered < toDeliver)
            {
                int remaining = toDeliver - delivered;
                int bufSize = 1 + random.nextInt(remaining);
                ByteBuf actual = allocator.buffer(bufSize);
                buffer.readBytes(actual);

                invoker.accept(actual);

                delivered += bufSize;
            }
        }

        @Override
        public ByteBuf alloc(int messageLength)
        {
            return allocator.buffer(messageLength);
        }

        @Override
        public void flush()
        {

        }
    }

    private static class OneGiantMessageChannel implements Channel, FlushableChannel
    {
        private final Consumer<ByteBuf> invoker;
        private final UnpooledByteBufAllocator allocator;
        private ByteBuf buffer;

        OneGiantMessageChannel(Consumer<ByteBuf> invoker)
        {
            this.invoker = invoker;
            this.allocator = new UnpooledByteBufAllocator(false, true);
            this.buffer = this.allocator.buffer(128);
        }

        @Override
        public void write(final ByteBuf inboundBuffer)
        {
            if (this.buffer.writableBytes() < inboundBuffer.readableBytes())
            {
                final ByteBuf newBuf = allocator.buffer(this.buffer.capacity() * 2);

                this.buffer.readBytes(newBuf, this.buffer.readableBytes());

                this.buffer = newBuf;
                write(inboundBuffer);
            }
            else
            {
                inboundBuffer.readBytes(this.buffer, inboundBuffer.readableBytes());
            }
        }

        @Override
        public ByteBuf alloc(int messageLength)
        {
            return allocator.buffer(messageLength);
        }

        @Override
        public void flush()
        {
            invoker.accept(buffer);
        }
    }
}
