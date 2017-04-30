package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import net.digihippo.timecache.TimeCacheAgent;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class AgentRoundTripTest
{
    @Rule
    public final JUnitRuleMockery mockery = new JUnitRuleMockery();

    private final TimeCacheAgent endpoint = mockery.mock(TimeCacheAgent.class);

    private final TimeCacheAgent timeCacheAgent =
        new RemoteNettyAgent(new Channel()
        {
            private final ByteBufAllocator allocator = new PooledByteBufAllocator();
            private final AgentInvoker invoker = new AgentInvoker(endpoint);

            @Override
            public void write(ByteBuf buffer)
            {
                invoker.dispatch(buffer);
            }

            @Override
            public ByteBuf alloc(int messageLength)
            {
                return allocator.buffer(messageLength);
            }
        });

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
