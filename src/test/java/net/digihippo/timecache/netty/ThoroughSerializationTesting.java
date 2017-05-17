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

public final class ThoroughSerializationTesting
{
    public static <T> void runTest(
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
        final Channel channel = new RandomFragmentChannel(random, invoker);
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
    }

    public static <T> List<T> generateList(
        final Random random, final Function<Random, T> generator)
    {
        int size = random.nextInt(200);
        final List<T> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            result.add(generator.apply(random));
        }

        return result;
    }

    private ThoroughSerializationTesting() {}

    private static class RandomFragmentChannel implements Channel
    {
        private final ByteBufAllocator allocator;
        private final Random random;
        private final Consumer<ByteBuf> invoker;

        public RandomFragmentChannel(Random random, Consumer<ByteBuf> invoker)
        {
            this.random = random;
            this.invoker = invoker;
            allocator = new UnpooledByteBufAllocator(false, true);
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
    }
}
