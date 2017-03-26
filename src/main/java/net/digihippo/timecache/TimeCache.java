package net.digihippo.timecache;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

class TimeCache implements TimeCacheServer {
    private final List<TimeCacheAgent> agents = new ArrayList<>();
    private final Map<String, CacheDefinition<?>> caches = new HashMap<>();

    public void load(
            String cacheName,
            ZonedDateTime fromInclusive,
            ZonedDateTime toExclusive,
            TimeUnit bucketSize) {
        long fromMillis = fromInclusive.toInstant().toEpochMilli();
        long toMillis = toExclusive.toInstant().toEpochMilli();
        // FIXME: imprecise intervals
        long bucketSizeMillis = bucketSize.toMillis(1L);
        long bucketCount = (toMillis - fromMillis) / bucketSizeMillis;
        CacheDefinition<?> cacheDefinition = caches.get(cacheName);

        long currentBucketStart = fromMillis;
        long currentBucketEnd = fromMillis + bucketSizeMillis;
        for (int i = 0; i < bucketCount; i++) {
            agents.get(i % agents.size()).populateBucket(cacheDefinition, currentBucketStart, currentBucketEnd);
            currentBucketStart = currentBucketEnd;
            currentBucketEnd += bucketSizeMillis;
        }
    }

    public <T, U> void iterate(
            String cacheName,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            ReductionDefinition<T, U> reductionDefinition) {
        for (TimeCacheAgent agent : agents) {
            agent.iterate(cacheName, from, toExclusive, reductionDefinition);
        }
    }

    @Override
    public void loadComplete(
            String agentId,
            CacheDefinition<?> cacheDefinition,
            long bucketStart,
            long bucketEnd)
    {

    }

    interface TimeCacheAgent {
        void populateBucket(
                TimeCache.CacheDefinition<?> cacheDefinition,
                long currentBucketStart,
                long currentBucketEnd);

        <U, T> void iterate(
                String cacheName,
                ZonedDateTime from,
                ZonedDateTime toExclusive,
                ReductionDefinition<T, U> definition);
    }

    public static class CacheDefinition<T>
    {
        public final String cacheName;
        public final Class<T> cacheClass;
        public final EventLoader<T> eventLoader;
        public final MillitimeExtractor<T> millitimeExtractor;

        public CacheDefinition(
                String cacheName,
                Class<T> cacheClass,
                EventLoader<T> eventLoader,
                MillitimeExtractor<T> millitimeExtractor) {

            this.cacheName = cacheName;
            this.cacheClass = cacheClass;
            this.eventLoader = eventLoader;
            this.millitimeExtractor = millitimeExtractor;
        }
    }

    public void addAgent(TimeCacheAgent timeCacheAgent) {
        agents.add(timeCacheAgent);
    }

    public <T> void defineCache(
            String cacheName,
            Class<T> cacheClass,
            EventLoader<T> eventLoader,
            MillitimeExtractor<T> millitimeExtractor) {
        caches.put(cacheName, new CacheDefinition<>(cacheName, cacheClass, eventLoader, millitimeExtractor));
    }
}
