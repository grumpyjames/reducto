package net.digihippo.timecache;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

class TimeCache implements TimeCacheServer {
    private final List<TimeCacheAgent> agents = new ArrayList<>();

    private final Map<String, DistributedCacheStatus<?>> caches = new HashMap<>();
    private IterationStatus iterationStatus = null;

    private static final class DistributedCacheStatus<T>
    {
        private final CacheDefinition<T> definition;
        private final Set<Long> loadingKeys;
        private final Map<Long, Set<String>> bucketOwners;

        private DistributedCacheStatus(
                CacheDefinition<T> definition,
                Set<Long> loadingKeys,
                Map<Long, Set<String>> bucketOwners) {
            this.definition = definition;
            this.loadingKeys = loadingKeys;
            this.bucketOwners = bucketOwners;
        }

        public void loadComplete(String agentId, long bucketStart) {
            loadingKeys.remove(bucketStart);
            bucketOwners.computeIfAbsent(bucketStart, (b) -> new HashSet<>()).add(agentId);
        }

        public void loading(long currentBucketStart) {
            loadingKeys.add(currentBucketStart);
        }
    }

    private static class IterationStatus<T, U>
    {
        private long requiredBuckets;
        private final U accumulator;
        private final ReductionDefinition<T, U> reductionDefinition;
        private final IterationListener<U> iterationListener;

        private IterationStatus(
                long requiredBuckets,
                U accumulator,
                ReductionDefinition<T, U> reductionDefinition,
                IterationListener<U> iterationListener) {
            this.requiredBuckets = requiredBuckets;
            this.accumulator = accumulator;
            this.reductionDefinition = reductionDefinition;
            this.iterationListener = iterationListener;
        }

        public void bucketComplete(String agentId, long currentBucketKey, Object result) {
            @SuppressWarnings("unchecked") final U fromRemote = (U) result;
            reductionDefinition.reduceMany.accept(this.accumulator, fromRemote);
            requiredBuckets--;

            if (requiredBuckets == 0)
            {
               iterationListener.onComplete.accept(accumulator);
            }
        }
    }

    public void load(
            String cacheName,
            ZonedDateTime fromInclusive,
            ZonedDateTime toExclusive) {
        final long fromMillis = fromInclusive.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final DistributedCacheStatus<?> distributedCacheStatus = caches.get(cacheName);
        // FIXME: imprecise intervals
        final long bucketSizeMillis = distributedCacheStatus.definition.bucketSize.toMillis(1L);
        final long bucketCount = (toMillis - fromMillis) / bucketSizeMillis;

        long currentBucketStart = fromMillis;
        long currentBucketEnd = fromMillis + bucketSizeMillis;
        for (int i = 0; i < bucketCount; i++) {
            distributedCacheStatus.loading(currentBucketStart);
            agents.get(i % agents.size())
                    .populateBucket(distributedCacheStatus.definition, currentBucketStart, currentBucketEnd);
            currentBucketStart = currentBucketEnd;
            currentBucketEnd += bucketSizeMillis;
        }
    }

    public <T, U> void iterate(
            String cacheName,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            ReductionDefinition<T, U> reductionDefinition,
            IterationListener<U> iterationListener) {
        DistributedCacheStatus<?> distributedCacheStatus = caches.get(cacheName);
        if (distributedCacheStatus == null)
        {
            iterationListener.onFatalError.accept(String.format("Cache '%s' not found", cacheName));
            return;
        }

        final long fromMillis = from.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final long bucketMillis = distributedCacheStatus.definition.bucketSize.toMillis(1L);
        // FIXME: Huge bugs lurk here.
        final long firstBucketKey = (fromMillis / bucketMillis) * bucketMillis;
        final long lastBucketKey = (toMillis / bucketMillis) * bucketMillis;
        final long requiredBucketCount =
                (lastBucketKey - firstBucketKey) / bucketMillis + (firstBucketKey == lastBucketKey ? 1:0);

        iterationStatus =
                new IterationStatus<>(
                        requiredBucketCount,
                        reductionDefinition.initialSupplier.get(),
                        reductionDefinition,
                        iterationListener);
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
        caches.get(cacheDefinition.cacheName)
                .loadComplete(agentId, bucketStart);
    }

    @Override
    public void bucketComplete(
            String agentId,
            long currentBucketKey,
            Object result) {
        iterationStatus.bucketComplete(agentId, currentBucketKey, result);
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
        public final TimeUnit bucketSize;

        public CacheDefinition(
                String cacheName,
                Class<T> cacheClass,
                EventLoader<T> eventLoader,
                MillitimeExtractor<T> millitimeExtractor,
                TimeUnit bucketSize) {

            this.cacheName = cacheName;
            this.cacheClass = cacheClass;
            this.eventLoader = eventLoader;
            this.millitimeExtractor = millitimeExtractor;
            this.bucketSize = bucketSize;
        }
    }

    public void addAgent(TimeCacheAgent timeCacheAgent) {
        agents.add(timeCacheAgent);
    }

    public <T> void defineCache(
            String cacheName,
            Class<T> cacheClass,
            EventLoader<T> eventLoader,
            MillitimeExtractor<T> millitimeExtractor,
            TimeUnit bucketSize) {
        caches.put(
                cacheName,
                new DistributedCacheStatus<>(
                        new CacheDefinition<>(cacheName, cacheClass, eventLoader, millitimeExtractor, bucketSize),
                        new HashSet<>(),
                        new HashMap<>()));
    }
}
