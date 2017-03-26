package net.digihippo.timecache;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;

public class InMemoryTimeCacheAgent implements TimeCache.TimeCacheAgent {
    private final String agentId;
    private final TimeCacheServer timeCacheServer;
    private final Map<String, Cache<?>> caches = new HashMap<>();

    public InMemoryTimeCacheAgent(String agentId, TimeCacheServer timeCacheServer) {
        this.agentId = agentId;
        this.timeCacheServer = timeCacheServer;
    }

    @Override
    public void populateBucket(
            TimeCache.CacheDefinition<?> cacheDefinition,
            long currentBucketStart,
            long currentBucketEnd) {
        Cache cache = caches.computeIfAbsent(
                cacheDefinition.cacheName,
                cacheName -> new Cache<>(cacheDefinition, currentBucketEnd - currentBucketStart));
        //noinspection unchecked
        cacheDefinition
                .eventLoader
                .loadEvents(
                        Instant.ofEpochMilli(currentBucketStart),
                        Instant.ofEpochMilli(currentBucketEnd),
                        cache.newBucket(currentBucketStart));

        timeCacheServer
                .loadComplete(
                        agentId,
                        cacheDefinition,
                        currentBucketStart,
                        currentBucketEnd);
    }

    @Override
    public <U, T> void iterate(
            String cacheName,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            ReductionDefinition<T, U> definition) {
        @SuppressWarnings("unchecked") Cache<T> cache = (Cache<T>) caches.get(cacheName);
        cache
            .iterate(from, toExclusive, definition);
    }

    public static class Cache<T> {
        private final TimeCache.CacheDefinition<T> cacheDefinition;
        private final long bucketSize;
        private final Map<Long, List<T>> buckets = new HashMap<>();

        public Cache(TimeCache.CacheDefinition<T> cacheDefinition, long bucketSizeMillis) {
            this.cacheDefinition = cacheDefinition;
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
                ReductionDefinition<T, U> definition)
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
                                        long eTime = cacheDefinition.millitimeExtractor.apply(e);
                                        return fromEpochMilli <= eTime && eTime < toEpochMilli;
                                    })
                                    .forEach(
                                        item ->
                                            definition.reduceOne.accept(item, definition.initialSupplier.get())));
                currentBucketKey += bucketSize;
            }
        }
    }
}
