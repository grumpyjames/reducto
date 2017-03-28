package net.digihippo.timecache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MultipleCacheTest {
    private final TimeCache timeCache = new TimeCache();
    private final ZonedDateTime beginningOfTime =
            ZonedDateTime.of(2016, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC"));

    private final List<NamedEvent> allEvents = createEvents(beginningOfTime);

    private List<NamedEvent> createEvents(ZonedDateTime beginningOfTime) {
        return Arrays.asList(
                NamedEvent.event(beginningOfTime.plusSeconds(1L), "one"),
                NamedEvent.event(beginningOfTime.plusMinutes(2L).plusSeconds(45), "two"),
                NamedEvent.event(beginningOfTime.plusMinutes(5L).plusSeconds(3), "three"),
                NamedEvent.event(beginningOfTime.plusMinutes(8L).plusSeconds(11), "four"));
    }

    private BlockableServer agentTwoToServerLink;

    @Before
    public void setup()
    {
        BlockableServer agentOneToServerLink = new BlockableServer(timeCache);
        timeCache.addAgent(new InMemoryTimeCacheAgent("agentOne", agentOneToServerLink));
        agentTwoToServerLink = new BlockableServer(timeCache);
        timeCache.addAgent(new InMemoryTimeCacheAgent("agentTwo", agentTwoToServerLink));


        timeCache.defineCache(
            "byMinute",
            NamedEvent.class,
            new HistoricalEventLoader(allEvents),
            (NamedEvent ne) -> ne.time.toEpochMilli(),
            TimeUnit.MINUTES
        );

        timeCache.defineCache(
            "byHour",
            NamedEvent.class,
            new HistoricalEventLoader(allEvents),
            (NamedEvent ne) -> ne.time.toEpochMilli(),
            TimeUnit.HOURS
        );
    }

    @Test
    public void loadMultipleCaches()
    {
        agentTwoToServerLink.block();

        final LoadCompleteDetector minuteCacheLoadDetector = new LoadCompleteDetector();
        timeCache.load(
            "byMinute",
            beginningOfTime,
            beginningOfTime.plusMinutes(10),
            new LoadListener(minuteCacheLoadDetector, Assert::fail));

        final LoadCompleteDetector hourCacheLoadDetector = new LoadCompleteDetector();
        timeCache.load(
            "byHour",
            beginningOfTime,
            beginningOfTime.plusMinutes(10),
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
        ZonedDateTime start = this.beginningOfTime;
        ZonedDateTime end = start.plusMinutes(10);
        timeCache.load("byMinute", start, end, new LoadListener(() -> {}, Assert::fail));
        timeCache.load("byHour", start, end, new LoadListener(() -> {}, Assert::fail));

        agentTwoToServerLink.block();

        final List<NamedEvent> minuteResults = new ArrayList<>();
        timeCache.iterate(
            "byMinute",
            start,
            end,
            new ReductionDefinition<>(
                ArrayList::new, (NamedEvent a, List<NamedEvent> b) -> b.add(a), List::addAll),
            new IterationListener<>(minuteResults::addAll, Assert::fail));

        final List<NamedEvent> hourResults = new ArrayList<>();
        timeCache.iterate(
            "byHour",
            start,
            end,
            new ReductionDefinition<>(
                ArrayList::new, (NamedEvent a, List<NamedEvent> b) -> b.add(a), List::addAll),
            new IterationListener<>(hourResults::addAll, Assert::fail));

        // The one bucketness of the hour cache should allow it to complete...
        assertThat(minuteResults, empty());
        assertThat(hourResults, containsInAnyOrder(allEvents.toArray()));

        agentTwoToServerLink.unblockAndFlush();

        assertThat(minuteResults, containsInAnyOrder(allEvents.toArray()));
        assertThat(hourResults, containsInAnyOrder(allEvents.toArray()));
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
        private final TimeCache.CacheDefinition cacheDefinition;
        private final long bucketStart;
        private final long bucketEnd;

        private LoadCompleteEvent(
            String agentId,
            TimeCache.CacheDefinition cacheDefinition,
            long bucketStart,
            long bucketEnd) {
            this.agentId = agentId;
            this.cacheDefinition = cacheDefinition;
            this.bucketStart = bucketStart;
            this.bucketEnd = bucketEnd;
        }

        @Override
        public void deliverTo(TimeCacheServer timeCache) {
            timeCache.loadComplete(agentId, cacheDefinition, bucketStart, bucketEnd);
        }
    }

    private static class BucketComplete implements TimeCacheEvent
    {
        private final String agentId;
        private final String cacheName;
        private final long iterationKey;
        private final long bucketKey;
        private final Object result;

        private BucketComplete(
            String agentId,
            String cacheName,
            long iterationKey,
            long bucketKey,
            Object result) {
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

        public BlockableServer(TimeCache timeCache) {
            this.timeCache = timeCache;
        }

        @Override
        public void loadComplete(
                String agentId,
                TimeCache.CacheDefinition<?> cacheDefinition,
                long bucketStart,
                long bucketEnd) {
            if (blocking) {
                events.add(new LoadCompleteEvent(agentId, cacheDefinition, bucketStart, bucketEnd));
            } else {
                timeCache.loadComplete(agentId, cacheDefinition, bucketStart, bucketEnd);
            }
        }

        @Override
        public void bucketComplete(
            String agentId,
            String cacheName,
            long iterationKey,
            long currentBucketKey,
            Object result) {
            if (blocking) {
                events.add(new BucketComplete(agentId, cacheName, iterationKey, currentBucketKey, result));
            } else {
                timeCache.bucketComplete(agentId, cacheName, iterationKey, currentBucketKey, result);
            }
        }

        public void block() {
            blocking = true;
        }

        public void unblockAndFlush() {
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
