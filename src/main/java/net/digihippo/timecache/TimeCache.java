package net.digihippo.timecache;

import java.lang.reflect.InvocationTargetException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

class TimeCache implements TimeCacheServer {
    private final Map<String, TimeCacheAgent> agents = new HashMap<>();

    private final Map<String, DistributedCacheStatus<?>> caches = new HashMap<>();
    private final Map<String, Map<String, ReductionDefinition<?, ?>>> definitions = new HashMap<>();
    private final Map<String, InstallationProgress> installationProgress = new HashMap<>();

    private static final class InstallationProgress
    {
        private final Set<String> remainingAgents;
        private final InstallationListener listener;
        private final Map<String, String> errors = new HashMap<>();

        private InstallationProgress(Set<String> remainingAgents, InstallationListener listener) {
            this.remainingAgents = remainingAgents;
            this.listener = listener;
        }

        public void complete(String agentName) {
            remainingAgents.remove(agentName);
            tryCompletion();
        }

        public void error(String agentName, String errorMessage) {
            remainingAgents.remove(agentName);
            errors.put(agentName, errorMessage);
            tryCompletion();
        }

        private void tryCompletion() {
            if (remainingAgents.isEmpty())
            {
                if (errors.isEmpty()) {
                    listener.onComplete.run();
                } else {
                    listener.onError.accept(errors);
                }
            }
        }
    }

    public void installDefinitions(String name, InstallationListener installationListener) {
        try {
            Object o = Class.forName(name).getConstructor().newInstance();
            DefinitionSource definitionSource = (DefinitionSource) o;
            Map<String, ReductionDefinition<?, ?>> definitions = definitionSource.definitions();
            this.definitions.put(name, definitions);
            installationProgress.put(
                name,
                new InstallationProgress(new HashSet<>(agents.keySet()), installationListener));
            agents.values().forEach(agent -> agent.installDefinitions(name));
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

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
            String installingClass,
            String definitionName,
            ReductionDefinition<T, U> reductionDefinition,
            IterationListener<U> iterationListener,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            Collection<TimeCacheAgent> agents) {

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
                agent.iterate(
                    definition.cacheName, newIterationKey, from, toExclusive, installingClass, definitionName);
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
            TimeCacheAgent[] values = agents.values().toArray(new TimeCacheAgent[agents.size()]);
            values[i % agents.size()]
                .populateBucket(distributedCacheStatus.definition, currentBucketStart, currentBucketEnd);
            currentBucketStart = currentBucketEnd;
            currentBucketEnd += bucketSizeMillis;
        }
    }

    public <T, U> void iterate(
            String cacheName,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            String definingClass,
            String iterateeName,
            IterationListener<U> iterationListener) {
        @SuppressWarnings("unchecked") DistributedCacheStatus<T> distributedCacheStatus =
            (DistributedCacheStatus<T>) caches.get(cacheName);
        if (distributedCacheStatus == null)
        {
            iterationListener.onFatalError.accept(String.format("Cache '%s' not found", cacheName));
            return;
        }

        @SuppressWarnings("unchecked") final ReductionDefinition<T, U> reductionDefinition =
            (ReductionDefinition<T, U>) definitions.get(definingClass).get(iterateeName);

        final long fromMillis = from.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final long bucketMillis = distributedCacheStatus.definition.bucketSize.toMillis(1L);
        final long firstBucketKey = (fromMillis / bucketMillis) * bucketMillis;
        final long lastBucketKey = (toMillis / bucketMillis) * bucketMillis;
        final long requiredBucketCount =
                (lastBucketKey - firstBucketKey) / bucketMillis + (firstBucketKey == lastBucketKey ? 1:0);

        distributedCacheStatus.launchNewIteration(
            requiredBucketCount,
            definingClass,
            iterateeName,
            reductionDefinition,
            iterationListener,
            from,
            toExclusive,
            agents.values());

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

    @Override
    public void installationComplete(String agentName, String installationKlass) {
        installationProgress.get(installationKlass).complete(agentName);
    }

    @Override
    public void installationError(
        String agentName,
        String installationKlass,
        String errorMessage) {
        installationProgress.get(installationKlass).error(agentName, errorMessage);
    }

    interface TimeCacheAgent {
        void installDefinitions(String className);

        void populateBucket(
                TimeCache.CacheDefinition<?> cacheDefinition,
                long currentBucketStart,
                long currentBucketEnd);

        <U, T> void iterate(
                String cacheName,
                long iterationKey,
                ZonedDateTime from,
                ZonedDateTime toExclusive,
                String installingClass,
                String definitionName);
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

    public void addAgent(String agentName, TimeCacheAgent timeCacheAgent) {
        agents.put(agentName, timeCacheAgent);
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
