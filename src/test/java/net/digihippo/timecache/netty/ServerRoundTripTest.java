package net.digihippo.timecache.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import net.digihippo.timecache.TimeCacheServer;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class ServerRoundTripTest
{
    @Rule
    public final JUnitRuleMockery mockery = new JUnitRuleMockery();

    private final TimeCacheServer endpoint = mockery.mock(TimeCacheServer.class);

    private final TimeCacheServer server =
        new RemoteNettyServer(new Channel()
        {
            private final ByteBufAllocator allocator = new PooledByteBufAllocator();
            private final ServerInvoker invoker = new ServerInvoker(endpoint);

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
    public void runThoroughTest()
    {
        List<Invocation<TimeCacheServer>> possibleInvocations = Arrays.asList(
            server -> server.loadComplete("foo", "bar", 234279L, 345387L),
            server -> {
                final ByteBuffer buffer = ByteBuffer.allocate(3);
                buffer.put(new byte[]{(byte) 6, (byte) 9, (byte) 1});
                buffer.flip();
                server.bucketComplete(
                    "foo", "bar", 325235L, 2523L, buffer);
            },
            server -> server.installationComplete("foo", "bar"),
            server -> server.installationError("foo", "bar", "oops"),
            server -> server.cacheDefined("banana", "pineapple")
        );
        ThoroughSerializationTesting.runTest(
            mockery,
            TimeCacheServer.class,
            tc -> new ServerInvoker(tc)::dispatch,
            RemoteNettyServer::new,
            possibleInvocations
        );
    }

    @Test
    public void roundTripLoadComplete()
    {
        mockery.checking(new Expectations() {{
            oneOf(endpoint).loadComplete("foo", "bar", 234279L, 345387L);
        }});

        server.loadComplete("foo", "bar", 234279L, 345387L);
    }

    @Test
    public void roundTripBucketComplete()
    {
        mockery.checking(new Expectations() {{
            oneOf(endpoint)
                .bucketComplete(
                    with("foo"),
                    with("bar"),
                    with(325235L),
                    with(2523L),
                    with(bytesEqual((byte) 6, (byte) 9, (byte) 1)));
        }});

        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(new byte[] { (byte) 6, (byte) 9, (byte) 1 });
        buffer.flip();
        server.bucketComplete(
            "foo", "bar", 325235L, 2523L, buffer);
    }

    @Test
    public void roundTripInstallationComplete()
    {
        mockery.checking(new Expectations() {{
            oneOf(endpoint)
                .installationComplete("foo", "bar");
        }});

        server.installationComplete("foo", "bar");
    }


    @Test
    public void roundTripInstallationFailure()
    {
        mockery.checking(new Expectations() {{
            oneOf(endpoint)
                .installationError("foo", "bar", "oops");
        }});

        server.installationError("foo", "bar", "oops");
    }

    private Matcher<ByteBuffer> bytesEqual(final byte...expected)
    {
        return new TypeSafeDiagnosingMatcher<ByteBuffer>()
        {
            @Override
            protected boolean matchesSafely(ByteBuffer byteBuffer, Description description)
            {
                for (int i = 0; i < expected.length; i++)
                {
                    byte expectedByte = expected[i];
                    byte actual = byteBuffer.get(i);
                    if (expectedByte != actual) {
                        description.appendText(
                            "Differed at byte " + i + "; expected " + expectedByte + ", got " + actual);
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo(Description description)
            {
                description.appendText("Byte buffer with byte content: ")
                    .appendText(Arrays.toString(expected));
            }
        };
    }
}
