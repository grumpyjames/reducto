package net.digihippo.timecache;

import net.digihippo.timecache.api.CacheComponentsFactory;
import net.digihippo.timecache.netty.NettyTimeCacheAgent;
import net.digihippo.timecache.netty.NettyTimeCacheClient;
import net.digihippo.timecache.netty.NettyTimeCacheServer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class EndToEndAcceptanceTest
{
    private static final int AGENT_COUNT = 10;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private static final ZonedDateTime BEGINNING_OF_TIME =
        ZonedDateTime.of(2016, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    private static final List<NamedEvent> ALL_EVENTS =
        createEvents(BEGINNING_OF_TIME);

    private static List<NamedEvent> createEvents(ZonedDateTime beginningOfTime) {
        final List<NamedEvent> events = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
        {
             events.add(NamedEvent.event(beginningOfTime.plusSeconds(i), Integer.toString(i)));
        }
        return events;
    }

    @SuppressWarnings("WeakerAccess") // accessed reflectively
    public static final class CacheDefinition implements CacheComponentsFactory<NamedEvent>
    {
        @Override
        public CacheComponents<NamedEvent> createCacheComponents()
        {
            return new CacheComponents<>(
                NamedEvent.class,
                new NamedEventSerializer(),
                new HistoricalEventLoader(ALL_EVENTS),
                ne -> ne.time.toEpochMilli(),
                TimeUnit.MINUTES);
        }
    }

    @Test
    public void runReductionAcrossMultipleNodesOnTheSameHost() throws InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(AGENT_COUNT);

        TimeCache timeCache = NettyTimeCacheServer.startTimeCacheServer(9191, noisyTimeCacheEvents(latch));

        for (int i = 0; i < AGENT_COUNT; i++)
        {
             executorService.execute(() -> {
                 try
                 {
                     NettyTimeCacheAgent.connectAndRunAgent("localhost", 9191);
                 } catch (InterruptedException e)
                 {
                     e.printStackTrace();
                 }
             });
        }

        latch.await();

        TimeCacheActions actions = NettyTimeCacheClient.connect("localhost", 9192);

        Consumer<String> throwIt = (error) -> {
            throw new RuntimeException(error);
        };

        final CountDownLatch latchTwo = new CountDownLatch(1);
        actions
            .defineCache(
                "scoot",
                CacheDefinition.class.getName(),
                new DefinitionListener(latchTwo::countDown, throwIt));
        latchTwo.await();

        final CountDownLatch latchTwoPointTwo = new CountDownLatch(1);

        actions
            .load(
                "scoot",
                BEGINNING_OF_TIME,
                BEGINNING_OF_TIME.plusMinutes(10),
                new LoadListener(latchTwoPointTwo::countDown, throwIt));

        latchTwoPointTwo.await();

        final CountDownLatch latchThree = new CountDownLatch(1);
        actions.installDefinitions(
            PlaybackDefinitions.class.getName(),
            new InstallationListener(latchThree::countDown, throwIt));

        latchThree.await();

        final CountDownLatch latchFour = new CountDownLatch(1);
        final ByteBuffer filterBuffer = ByteBuffer.allocate(5);
        filterBuffer.putInt(1);
        filterBuffer.put("6".getBytes(StandardCharsets.UTF_8));
        filterBuffer.flip();

        actions
            .iterate(
                "scoot",
                BEGINNING_OF_TIME.plusSeconds(7),
                BEGINNING_OF_TIME.plusMinutes(8),
                PlaybackDefinitions.class.getName(),
                "default",
                Optional.of(filterBuffer),
                new IterationListener(o -> {
                    List<NamedEvent> events =
                        new ListSerializer<>(new NamedEventSerializer()).decode(o);
                    assertThat(events.size(), equalTo(381));
                    System.out.println("Found " + events.size() + " events");
                    events.forEach(System.out::println);
                    latchFour.countDown();
                }, throwIt));

        latchFour.await();
        System.out.println("yay!");
    }

    private TimeCacheEvents noisyTimeCacheEvents(final CountDownLatch latch)
    {
        return new TimeCacheEvents() {
            @Override
            public void onAgentConnected()
            {
                latch.countDown();
            }

            @Override
            public void loadComplete(String agentId, String cacheName, long bucketStart, long bucketEnd)
            {
//                System.out.println(agentId + " loaded " + cacheName + " from " + bucketStart + " to " + bucketEnd);
            }

            @Override
            public void definitionsInstalled(String name)
            {
//                System.out.println("Definitions installed " + name);
            }

            @Override
            public void definitionsInstalled(String agentName, String installationKlass)
            {
//                System.out.println("Agent " + agentName + " installed definition " + installationKlass);
            }

            @Override
            public void iterationBucketComplete(String agentId, String cacheName, long iterationKey, long currentBucketKey)
            {
//                System.out.println("Agent " + agentId + " completed bucket " + currentBucketKey + " for iteration " + iterationKey);
            }
        };
    }
}
