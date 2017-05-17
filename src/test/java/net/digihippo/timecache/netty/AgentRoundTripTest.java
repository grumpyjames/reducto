package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import net.digihippo.timecache.TimeCacheAgent;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class AgentRoundTripTest
{
    @Rule
    public final JUnitRuleMockery mockery = new JUnitRuleMockery();

    private final TimeCacheAgent endpoint = mockery.mock(TimeCacheAgent.class);

    private final TimeCacheAgent timeCacheAgent =
        newRemoteAgent(new Random(42352523L));

    private RemoteNettyAgent newRemoteAgent(final Random random)
    {
        return new RemoteNettyAgent(new Channel()
        {
            private final ByteBufAllocator allocator = new UnpooledByteBufAllocator(false, true);
            private final AgentInvoker invoker = new AgentInvoker(endpoint);

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

                    invoker.dispatch(actual);

                    delivered += bufSize;
                }
            }

            @Override
            public ByteBuf alloc(int messageLength)
            {
                return allocator.buffer(messageLength);
            }
        });
    }

    private final List<Invocation> invocations =
        Arrays.asList(
            new Invocation()
            {

                @Override
                public void installExpectation(Mockery mockery)
                {
                    mockery.checking(
                        new Expectations()
                        {{
                            oneOf(endpoint).defineCache("foo", "bar");
                        }}
                    );
                }

                @Override
                public void run(TimeCacheAgent agent)
                {
                    timeCacheAgent.defineCache("foo", "bar");
                }
            },
            new Invocation()
            {
                @Override
                public void installExpectation(Mockery mockery)
                {
                    mockery.checking(
                        new Expectations()
                        {{
                            oneOf(endpoint).installDefinitions("foo");
                        }}
                    );
                }

                @Override
                public void run(TimeCacheAgent agent)
                {
                    timeCacheAgent.installDefinitions("foo");
                }
            },
            new Invocation()
            {
                @Override
                public void installExpectation(Mockery mockery)
                {
                    mockery.checking(
                        new Expectations()
                        {{
                            oneOf(endpoint)
                                .iterate(
                                    "foo",
                                    252252L,
                                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(645646L), ZoneId.of("UTC")),
                                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(54964797289L), ZoneId.of("UTC")),
                                    "bar",
                                    "baz");
                        }}
                    );
                }

                @Override
                public void run(TimeCacheAgent agent)
                {
                    timeCacheAgent.iterate(
                        "foo",
                        252252L,
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(645646L), ZoneId.of("UTC")),
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(54964797289L), ZoneId.of("UTC")),
                        "bar",
                        "baz");
                }
            },
            new Invocation()
            {
                @Override
                public void installExpectation(Mockery mockery)
                {
                    mockery.checking(
                        new Expectations()
                        {{
                            oneOf(endpoint)
                                .populateBucket(
                                    "foo",
                                    23298L,
                                    2352L);
                        }}
                    );
                }

                @Override
                public void run(TimeCacheAgent agent)
                {
                    timeCacheAgent.populateBucket("foo", 23298L, 2352L);
                }
            }
        );

    private interface Invocation
    {
        void installExpectation(final Mockery mockery);
        void run(final TimeCacheAgent agent);
    }

    private <T> List<T> generateList(final Random random, final Function<Random, T> generator)
    {
        int size = random.nextInt(200);
        final List<T> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
             result.add(generator.apply(random));
        }

        return result;
    }

    private Invocation generateInvocation(final Random random)
    {
        int i = random.nextInt(4);
        return invocations.get(i);
    }

    @Test
    public void tortureTest()
    {
        final Random random = new Random(System.currentTimeMillis());
        final TimeCacheAgent agent = newRemoteAgent(random);

        final List<Invocation> invocations = generateList(random, this::generateInvocation);
        System.out.println("Running test with " + invocations.size() + " events");
        for (Invocation invocation : invocations)
        {
            invocation.installExpectation(mockery);
            invocation.run(agent);
        }
    }

    @Test
    public void roundTripDefineCache()
    {
        mockery.checking(
            new Expectations()
            {{
                oneOf(endpoint).defineCache("foo", "bar");
            }}
        );

        timeCacheAgent.defineCache("foo", "bar");
    }


    @Test
    public void roundTripInstallDefinitions()
    {
        mockery.checking(
            new Expectations()
            {{
                oneOf(endpoint).installDefinitions("foo");
            }}
        );

        timeCacheAgent.installDefinitions("foo");
    }

    @Test
    public void roundTripIterate()
    {
        mockery.checking(
            new Expectations()
            {{
                oneOf(endpoint)
                    .iterate(
                        "foo",
                        252252L,
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(645646L), ZoneId.of("UTC")),
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(54964797289L), ZoneId.of("UTC")),
                        "bar",
                        "baz");
            }}
        );

        timeCacheAgent.iterate(
            "foo",
            252252L,
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(645646L), ZoneId.of("UTC")),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(54964797289L), ZoneId.of("UTC")),
            "bar",
            "baz");
    }

    @Test
    public void roundTripPopulateBucket()
    {
        mockery.checking(
            new Expectations()
            {{
                oneOf(endpoint)
                    .populateBucket(
                        "foo",
                        23298L,
                        2352L);
            }}
        );


        timeCacheAgent.populateBucket("foo", 23298L, 2352L);
    }
}
