package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import net.digihippo.timecache.TimeCacheAgent;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static net.digihippo.timecache.netty.ThoroughSerializationTesting.runTest;

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

    private static final List<Invocation<TimeCacheAgent>> ALL_POSSIBLE_INVOCATIONS =
        Arrays.asList(
            agent -> agent.defineCache("foo", "bar"),
            agent -> agent.installDefinitions("foo"),
            agent -> agent.iterate(
                "foo",
                252252L,
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(645646L), ZoneId.of("UTC")),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(54964797289L), ZoneId.of("UTC")),
                "bar",
                "baz",
                Optional.empty()),
            agent -> agent.populateBucket("foo", 23298L, 2352L)
        );

    @Test
    public void runThoroughTest()
    {
        runTest(
            mockery,
            TimeCacheAgent.class,
            endpoint -> new AgentInvoker(endpoint)::dispatch,
            RemoteNettyAgent::new,
            ALL_POSSIBLE_INVOCATIONS);
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
                        "baz",
                        Optional.empty());
            }}
        );

        timeCacheAgent.iterate(
            "foo",
            252252L,
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(645646L), ZoneId.of("UTC")),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(54964797289L), ZoneId.of("UTC")),
            "bar",
            "baz",
            Optional.empty());
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
