package net.digihippo.timecache;

import net.digihippo.timecache.api.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.*;

public class MultipleConcurrentActionsAcrossMultipleCachesTest {
    private static final ZonedDateTime BEGINNING_OF_TIME =
            ZonedDateTime.of(2016, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    private static final List<NamedEvent> ALL_EVENTS = createEvents(BEGINNING_OF_TIME);

    private final TimeCache timeCache = new TimeCache(TimeCacheEvents.NO_OP);

    private static List<NamedEvent> createEvents(ZonedDateTime beginningOfTime) {
        return Arrays.asList(
                NamedEvent.event(beginningOfTime.plusSeconds(1L), "one"),
                NamedEvent.event(beginningOfTime.plusMinutes(2L).plusSeconds(45), "two"),
                NamedEvent.event(beginningOfTime.plusMinutes(5L).plusSeconds(3), "three"),
                NamedEvent.event(beginningOfTime.plusMinutes(8L).plusSeconds(11), "four"));
    }

    private BlockableServer agentTwoToServerLink;

    @SuppressWarnings("WeakerAccess") // loaded reflectively
    public static class Definitions implements DefinitionSource
    {
        @Override
        public Map<String, ReductionDefinition<?, ?, ?>> definitions() {
            HashMap<String, ReductionDefinition<?, ?, ?>> result = new HashMap<>();
            result.put("default",
                new ReductionDefinition<NamedEvent, List<NamedEvent>, Void>(
                    ArrayList::new,
                    List::add,
                    List::addAll,
                    new ListSerializer<>(new NamedEventSerializer()),
                    new FilterDefinition<>(
                        new Serializer<Void>()
                        {
                            @Override
                            public void encode(Void aVoid, WriteBuffer bb)
                            {

                            }

                            @Override
                            public Void decode(ByteBuffer bb)
                            {
                                return null;
                            }
                        },
                        aVoid -> namedEvent -> true
                    )));
            return result;
        }
    }

    @SuppressWarnings("WeakerAccess") // loaded reflectively
    public static final class MinuteCacheFactory implements CacheComponentsFactory<NamedEvent>
    {
        @Override
        public CacheComponents<NamedEvent> createCacheComponents() {
            return new CacheComponents<>(
                NamedEvent.class,
                new NamedEventSerializer(),
                new HistoricalEventLoader(ALL_EVENTS),
                (NamedEvent ne) -> ne.time.toEpochMilli(),
                TimeUnit.MINUTES);
        }
    }

    @SuppressWarnings("WeakerAccess") // loaded reflectively
    public static final class HourCacheFactory implements CacheComponentsFactory<NamedEvent>
    {
        @Override
        public CacheComponents<NamedEvent> createCacheComponents() {
            return new CacheComponents<>(
                NamedEvent.class,
                new NamedEventSerializer(),
                new HistoricalEventLoader(ALL_EVENTS),
                (NamedEvent ne) -> ne.time.toEpochMilli(),
                TimeUnit.HOURS);
        }
    }

    @Before
    public void setup()
    {
        BlockableServer agentOneToServerLink = new BlockableServer(timeCache);
        timeCache.addAgent(
            "agentOne", new InMemoryTimeCacheAgent("agentOne", agentOneToServerLink));
        agentTwoToServerLink = new BlockableServer(timeCache);
        timeCache.addAgent(
            "agentTwo", new InMemoryTimeCacheAgent("agentTwo", agentTwoToServerLink));


        timeCache.defineCache(
            "byMinute",
            MinuteCacheFactory.class.getName(),
            new DefinitionListener((e) -> { throw new RuntimeException(e); }, () -> {}));

        timeCache.defineCache(
            "byHour",
            HourCacheFactory.class.getName(),
            new DefinitionListener((e) -> { throw new RuntimeException(e); }, () -> {}));
    }

    @Test
    public void loadMultipleCaches()
    {
        agentTwoToServerLink.block();

        final LoadCompleteDetector minuteCacheLoadDetector = new LoadCompleteDetector();
        timeCache.load(
            "byMinute",
            BEGINNING_OF_TIME,
            BEGINNING_OF_TIME.plusMinutes(10),
            new LoadListener(minuteCacheLoadDetector, Assert::fail));

        final LoadCompleteDetector hourCacheLoadDetector = new LoadCompleteDetector();
        timeCache.load(
            "byHour",
            BEGINNING_OF_TIME,
            BEGINNING_OF_TIME.plusMinutes(10),
            new LoadListener(hourCacheLoadDetector, Assert::fail));

        assertTrue(hourCacheLoadDetector.loadComplete);
        assertFalse(
            "Minute cache should not finish loading until agent two reports in",
            minuteCacheLoadDetector.loadComplete);

        agentTwoToServerLink.unblockAndFlush();
    }

    @Test
    public void iterateOverMultipleCaches()
    {
        ZonedDateTime start = BEGINNING_OF_TIME;
        ZonedDateTime end = start.plusMinutes(10);
        timeCache.load("byMinute", start, end, new LoadListener(() -> {}, Assert::fail));
        timeCache.load("byHour", start, end, new LoadListener(() -> {}, Assert::fail));

        agentTwoToServerLink.block();

        timeCache.installDefinitions(
            Definitions.class.getName(),
            new InstallationListener(() -> {}, m -> Assert.fail("found installation errors: " + m.toString())));

        final List<NamedEvent> minuteResults = new ArrayList<>();
        timeCache.iterate(
            "byMinute",
            start,
            end,
            Definitions.class.getName(),
            "default",
            Optional.empty(),
            new IterationListener(
                (bb) -> minuteResults.addAll(new ListSerializer<>(new NamedEventSerializer()).decode(bb)),
                Assert::fail));

        final List<NamedEvent> hourResults = new ArrayList<>();
        timeCache.iterate(
            "byHour",
            start,
            end,
            Definitions.class.getName(),
            "default",
            Optional.empty(),
            new IterationListener(
                (bb) -> hourResults.addAll(new ListSerializer<>(new NamedEventSerializer()).decode(bb)),
                Assert::fail));

        // The one bucketness of the hour cache should allow it to complete...
        assertThat(minuteResults, empty());
        assertThat(hourResults, containsInAnyOrder(ALL_EVENTS.toArray()));

        agentTwoToServerLink.unblockAndFlush();

        assertThat(minuteResults, containsInAnyOrder(ALL_EVENTS.toArray()));
        assertThat(hourResults, containsInAnyOrder(ALL_EVENTS.toArray()));
    }

    private static class LoadCompleteDetector implements Runnable
    {
        private boolean loadComplete = false;

        @Override
        public void run() {
            loadComplete = true;
        }
    }

    private interface TimeCacheEvent
    {
        void deliverTo(TimeCacheServer timeCache);
    }

    private static class LoadCompleteEvent implements TimeCacheEvent
    {
        private final String agentId;
        private final String cacheName;
        private final long bucketStart;
        private final long bucketEnd;

        private LoadCompleteEvent(
            String agentId,
            String cacheName,
            long bucketStart,
            long bucketEnd) {
            this.agentId = agentId;
            this.cacheName = cacheName;
            this.bucketStart = bucketStart;
            this.bucketEnd = bucketEnd;
        }

        @Override
        public void deliverTo(TimeCacheServer timeCache) {
            timeCache.loadComplete(agentId, cacheName, bucketStart, bucketEnd);
        }
    }

    private static class BucketComplete implements TimeCacheEvent
    {
        private final String agentId;
        private final String cacheName;
        private final long iterationKey;
        private final long bucketKey;
        private final ByteBuffer result;

        private BucketComplete(
            String agentId,
            String cacheName,
            long iterationKey,
            long bucketKey,
            ByteBuffer result) {
            this.agentId = agentId;
            this.cacheName = cacheName;
            this.iterationKey = iterationKey;
            this.bucketKey = bucketKey;
            this.result = result;
        }

        @Override
        public void deliverTo(TimeCacheServer timeCache) {
            timeCache.bucketComplete(agentId, cacheName, iterationKey, bucketKey, result);
        }
    }

    private class BlockableServer implements TimeCacheServer {
        private final TimeCache timeCache;
        private final Queue<TimeCacheEvent> events = new ArrayDeque<>();
        private boolean blocking = false;

        BlockableServer(TimeCache timeCache) {
            this.timeCache = timeCache;
        }

        @Override
        public void loadComplete(
                String agentId,
                String cacheName,
                long bucketStart,
                long bucketEnd) {
            if (blocking) {
                events.add(new LoadCompleteEvent(agentId, cacheName, bucketStart, bucketEnd));
            } else {
                timeCache.loadComplete(agentId, cacheName, bucketStart, bucketEnd);
            }
        }

        @Override
        public void bucketComplete(
            String agentId,
            String cacheName,
            long iterationKey,
            long currentBucketKey,
            ByteBuffer result) {
            if (blocking) {
                events.add(new BucketComplete(agentId, cacheName, iterationKey, currentBucketKey, result));
            } else {
                timeCache.bucketComplete(agentId, cacheName, iterationKey, currentBucketKey, result);
            }
        }

        @Override
        public void installationComplete(String agentName, String installationKlass) {
            timeCache.installationComplete(agentName, installationKlass);
        }

        @Override
        public void installationError(String agentName, String installationKlass, String errorMessage) {
            timeCache.installationError(agentName, installationKlass, errorMessage);
        }

        @Override
        public void cacheDefined(String agentId, String cacheName)
        {
            timeCache.cacheDefined(agentId, cacheName);
        }

        void block() {
            blocking = true;
        }

        void unblockAndFlush() {
            blocking = false;
            TimeCacheEvent event = events.poll();
            while (event != null)
            {
                event.deliverTo(timeCache);
                event = events.poll();
            }
        }
    }


}
