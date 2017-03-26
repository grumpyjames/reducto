package net.digihippo.timecache;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class TimeCache {
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
            Class<T> cacheKlass,
            LongFunction<T> timeExtractor,
            U result,
            BiConsumer<T, U> reduceOne,
            BiConsumer<U, U> combiner) {
        for (TimeCacheAgent agent : agents) {
            agent.iterate(cacheName, from, toExclusive, timeExtractor, result, reduceOne, combiner);
        }
    }

    interface LongFunction<T>
    {
        long apply(final T t);
    }

    public static class Cache<T> {
        private final long bucketSize;
        private final Map<Long, List<T>> buckets = new HashMap<>();

        public Cache(long bucketSizeMillis) {
            this.bucketSize = bucketSizeMillis;
        }

        public Consumer<T> newBucket(long bucketStart) {
            List list = buckets.computeIfAbsent(bucketStart, bs -> new ArrayList());
            //noinspection unchecked
            return list::add;
        }

        public <U> void iterate(
                ZonedDateTime from,
                ZonedDateTime toExclusive,
                LongFunction<T> timeExtractor,
                U result,
                BiConsumer<T, U> reduceOne,
                BiConsumer<U, U> combiner)
        {
            long fromEpochMilli = from.toInstant().toEpochMilli();
            long toEpochMilli = toExclusive.toInstant().toEpochMilli();
            long currentBucketKey = (fromEpochMilli / bucketSize) * bucketSize;
            while (currentBucketKey < toEpochMilli)
            {
                Optional
                    .ofNullable(buckets.get(currentBucketKey))
                    .ifPresent(
                        items ->
                            items
                                .stream()
                                .filter(e -> {
                                    long eTime = timeExtractor.apply(e);
                                    return fromEpochMilli <= eTime && eTime < toEpochMilli;
                                })
                                .forEach(item -> reduceOne.accept(item, result)));
                currentBucketKey += bucketSize;
            }
        }
    }

    public static class InMemoryTimeCacheAgent implements TimeCacheAgent {
        public final Map<String, Cache<?>> caches = new HashMap<>();

        @Override
        public void populateBucket(
                CacheDefinition<?> cacheDefinition,
                long currentBucketStart,
                long currentBucketEnd) {
            Cache cache = caches.computeIfAbsent(
                    cacheDefinition.cacheName,
                    cacheName -> new Cache(currentBucketEnd - currentBucketStart));
            //noinspection unchecked
            cacheDefinition
                    .eventLoader
                    .loadEvents(
                            Instant.ofEpochMilli(currentBucketStart),
                            Instant.ofEpochMilli(currentBucketEnd),
                            cache.newBucket(currentBucketStart));
        }

        @Override
        public <U, T> void iterate(
                String cacheName,
                ZonedDateTime from,
                ZonedDateTime toExclusive,
                LongFunction<T> timeExtractor,
                U result,
                BiConsumer<T, U> reduceOne,
                BiConsumer<U, U> combiner) {
            @SuppressWarnings("unchecked") Cache<T> cache = (Cache<T>) caches.get(cacheName);
            cache
                .iterate(from, toExclusive, timeExtractor, result, reduceOne, combiner);
        }
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
                TimeCache.LongFunction<T> timeExtractor,
                U result,
                BiConsumer<T, U> reduceOne,
                BiConsumer<U, U> combiner);
    }

    private static class CacheDefinition<T>
    {
        private final String cacheName;
        private final Class<T> cacheClass;
        private final EventLoader<T> eventLoader;

        public CacheDefinition(String cacheName, Class<T> cacheClass, EventLoader<T> eventLoader) {

            this.cacheName = cacheName;
            this.cacheClass = cacheClass;
            this.eventLoader = eventLoader;
        }
    }

    public void addAgent(TimeCacheAgent timeCacheAgent) {
        agents.add(timeCacheAgent);
    }

    public <T> void defineCache(String cacheName, Class<T> cacheClass, EventLoader<T> eventLoader) {
        caches.put(cacheName, new CacheDefinition<>(cacheName, cacheClass, eventLoader));
    }
}
