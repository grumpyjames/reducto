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
        if (cache != null) {
            cache.iterate(agentId, from, toExclusive, definition, timeCacheServer);
        }
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
                String agentId,
                ZonedDateTime from,
                ZonedDateTime toExclusive,
                ReductionDefinition<T, U> definition,
                TimeCacheServer server)
        {
            long fromEpochMilli = from.toInstant().toEpochMilli();
            long toEpochMilli = toExclusive.toInstant().toEpochMilli();
            long bucketKey = (fromEpochMilli / bucketSize) * bucketSize;
            while (bucketKey < toEpochMilli)
            {
                final long currentBucketKey = bucketKey;
                Optional
                        .ofNullable(buckets.get(bucketKey))
                        .ifPresent(
                            items -> {
                                U result = definition.initialSupplier.get();
                                items
                                    .stream()
                                    .filter(e -> {
                                        long eTime = cacheDefinition.millitimeExtractor.apply(e);
                                        return fromEpochMilli <= eTime && eTime < toEpochMilli;
                                    })
                                    .forEach(
                                            item -> definition.reduceOne.accept(item, result));
                                server.bucketComplete(agentId, currentBucketKey, result);
                            });
                bucketKey += bucketSize;
            }
        }
    }
}
