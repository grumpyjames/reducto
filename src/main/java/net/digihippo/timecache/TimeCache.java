package net.digihippo.timecache;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class TimeCache implements TimeCacheServer {
    private final List<TimeCacheAgent> agents = new ArrayList<>();

    private final Map<String, DistributedCacheStatus<?>> caches = new HashMap<>();

    private static final class DistributedCacheStatus<T>
    {
        private final CacheDefinition<T> definition;
        private final Set<Long> loadingKeys;
        private final Map<Long, Set<String>> bucketOwners;
        private final Map<Long, IterationStatus> iterations = new HashMap<>();

        private long iterationKey;
        private LoadListener loadListener;
        private long bucketsLoading;

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
            --bucketsLoading;
            bucketOwners.computeIfAbsent(bucketStart, (b) -> new HashSet<>()).add(agentId);
            if (bucketsLoading == 0)
            {
                loadListener.onComplete.run();
            }
        }

        public void loading(long currentBucketStart, LoadListener loadListener) {
            loadingKeys.add(currentBucketStart);
            this.loadListener = loadListener;
        }

        public void loadingStarted(long bucketCount) {
            this.bucketsLoading = bucketCount;
        }

        public <U> void launchNewIteration(
            long requiredBucketCount,
            ReductionDefinition<T, U> reductionDefinition,
            IterationListener<U> iterationListener,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            List<TimeCacheAgent> agents) {

            long newIterationKey = iterationKey;
            iterations.put(
                newIterationKey,
                new IterationStatus<>(
                    newIterationKey,
                    requiredBucketCount,
                    reductionDefinition.initialSupplier.get(),
                    reductionDefinition,
                    iterationListener
                ));
            for (TimeCacheAgent agent : agents) {
                agent.iterate(definition.cacheName, newIterationKey, from, toExclusive, reductionDefinition);
            }
            iterationKey++;
        }

        public void bucketComplete(String agentId, long iterationKey, long currentBucketKey, Object result) {
            iterations
                .get(iterationKey)
                .bucketComplete(agentId, currentBucketKey, result);
        }
    }

    private static class IterationStatus<T, U>
    {
        private final long iterationKey;
        private long requiredBuckets;
        private final U accumulator;
        private final ReductionDefinition<T, U> reductionDefinition;
        private final IterationListener<U> iterationListener;

        private IterationStatus(
            long iterationKey,
            long requiredBuckets,
            U accumulator,
            ReductionDefinition<T, U> reductionDefinition,
            IterationListener<U> iterationListener) {
            this.iterationKey = iterationKey;
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
            ZonedDateTime toExclusive,
            LoadListener loadListener) {
        final DistributedCacheStatus<?> distributedCacheStatus = caches.get(cacheName);
        if (distributedCacheStatus == null)
        {
            loadListener.onFatalError.accept(String.format("Cache '%s' not found", cacheName));
            return;
        }

        final long fromMillis = fromInclusive.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final long bucketSizeMillis = distributedCacheStatus.definition.bucketSize.toMillis(1L);

        final long firstBucketKey = (fromMillis / bucketSizeMillis) * bucketSizeMillis;
        final long maybeLastBucketKey = (toMillis / bucketSizeMillis * bucketSizeMillis);
        final long lastBucketKey = maybeLastBucketKey == toMillis ? toMillis - bucketSizeMillis : maybeLastBucketKey;
        final long bucketCount = 1 + (lastBucketKey - firstBucketKey) / bucketSizeMillis;

        long currentBucketStart = firstBucketKey;
        long currentBucketEnd = currentBucketStart + bucketSizeMillis;
        distributedCacheStatus.loadingStarted(bucketCount);
        for (int i = 0; i < bucketCount; i++) {
            distributedCacheStatus.loading(currentBucketStart, loadListener);
            agents
                .get(i % agents.size())
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
        @SuppressWarnings("unchecked") DistributedCacheStatus<T> distributedCacheStatus =
            (DistributedCacheStatus<T>) caches.get(cacheName);
        if (distributedCacheStatus == null)
        {
            iterationListener.onFatalError.accept(String.format("Cache '%s' not found", cacheName));
            return;
        }

        final long fromMillis = from.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final long bucketMillis = distributedCacheStatus.definition.bucketSize.toMillis(1L);
        final long firstBucketKey = (fromMillis / bucketMillis) * bucketMillis;
        final long lastBucketKey = (toMillis / bucketMillis) * bucketMillis;
        final long requiredBucketCount =
                (lastBucketKey - firstBucketKey) / bucketMillis + (firstBucketKey == lastBucketKey ? 1:0);

        distributedCacheStatus.launchNewIteration(
            requiredBucketCount, reductionDefinition, iterationListener, from, toExclusive, agents);

    }

    @Override
    public void loadComplete(
            String agentId,
            CacheDefinition<?> cacheDefinition,
            long bucketStart,
            long bucketEnd)
    {
        caches
            .get(cacheDefinition.cacheName)
            .loadComplete(agentId, bucketStart);
    }

    @Override
    public void bucketComplete(
        String agentId,
        String cacheName,
        long iterationKey,
        long currentBucketKey,
        Object result) {
        caches
            .get(cacheName)
            .bucketComplete(agentId, iterationKey, currentBucketKey, result);
    }

    interface TimeCacheAgent {
        void populateBucket(
                TimeCache.CacheDefinition<?> cacheDefinition,
                long currentBucketStart,
                long currentBucketEnd);

        <U, T> void iterate(
                String cacheName,
                long iterationKey,
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
