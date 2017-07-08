package net.digihippo.timecache;

import net.digihippo.timecache.api.*;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.*;

public class TimeCache implements TimeCacheServer, Stoppable, TimeCacheActions
{
    private final Map<String, TimeCacheAgent> agents = new HashMap<>();
    private final Map<String, DefinitionStatus> definitionStatuses = new HashMap<>();
    private final Map<String, DistributedCacheStatus<?>> caches = new HashMap<>();
    private final Map<String, Map<String, ReductionDefinition<?, ?, ?>>> definitions = new HashMap<>();
    private final Map<String, InstallationProgress> installationProgress = new HashMap<>();
    private final List<Stoppable> stoppables = new ArrayList<>();
    private final TimeCacheEvents timeCacheEvents;

    public TimeCache(TimeCacheEvents timeCacheEvents)
    {
        this.timeCacheEvents = timeCacheEvents;
    }

    @Override
    public void stop()
    {
        stoppables.forEach(Stoppable::stop);
    }

    public void addShutdownHook(Stoppable stoppable)
    {
        stoppables.add(stoppable);
    }

    private static final class InstallationProgress
    {
        private final Set<String> remainingAgents;
        private final InstallationListener listener;
        private final Map<String, String> errors = new HashMap<>();

        private InstallationProgress(Set<String> remainingAgents, InstallationListener listener)
        {
            this.remainingAgents = remainingAgents;
            this.listener = listener;
        }

        void complete(String agentName)
        {
            remainingAgents.remove(agentName);
            tryCompletion();
        }

        void error(String agentName, String errorMessage)
        {
            remainingAgents.remove(agentName);
            errors.put(agentName, errorMessage);
            tryCompletion();
        }

        private void tryCompletion()
        {
            if (remainingAgents.isEmpty())
            {
                if (errors.isEmpty())
                {
                    listener.onComplete.run();
                }
                else
                {
                    listener.onError.accept(errorMessage(errors));
                }
            }
        }

        private String errorMessage(Map<String, String> errors)
        {
            final StringBuilder result = new StringBuilder("Installation failed due to: ");
            for (Map.Entry<String, String> errorByAgent : errors.entrySet())
            {
                result.append(errorByAgent.getKey()).append(": ").append(errorByAgent.getValue()).append("\n");
            }
            return result.toString();
        }
    }

    @Override
    public void installDefinitions(String name, InstallationListener installationListener)
    {
        Result<DefinitionSource, Exception> result = ClassLoading.loadAndCast(name, DefinitionSource.class);
        result.consume(
            definitionSource -> {
                timeCacheEvents.definitionsInstalled(name);
                Map<String, ReductionDefinition<?, ?, ?>> definitions = definitionSource.definitions();
                this.definitions.put(name, definitions);
                installationProgress.put(
                    name,
                    new InstallationProgress(new HashSet<>(agents.keySet()), installationListener));
                agents.values().forEach(agent -> agent.installDefinitions(name));
            },
            exc -> {
                throw new RuntimeException(exc);
            });
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
        private final List<String> loadErrors = new ArrayList<>();

        private DistributedCacheStatus(
            CacheDefinition<T> definition,
            Set<Long> loadingKeys,
            Map<Long, Set<String>> bucketOwners)
        {
            this.definition = definition;
            this.loadingKeys = loadingKeys;
            this.bucketOwners = bucketOwners;
        }

        public void loadComplete(String agentId, long bucketStart)
        {
            loadingKeys.remove(bucketStart);
            --bucketsLoading;
            bucketOwners.computeIfAbsent(bucketStart, (b) -> new HashSet<>()).add(agentId);
            checkCompletion();
        }

        void loadFailure(String agentId, long bucketStart, String message)
        {
            loadingKeys.remove(bucketStart);
            --bucketsLoading;
            loadErrors.add("Agent " + agentId + " failed to load bucket " + bucketStart + " due to " + message);
            checkCompletion();
        }

        private void checkCompletion()
        {
            if (bucketsLoading == 0)
            {
                if (loadErrors.isEmpty())
                {
                    loadListener.onComplete.run();
                }
                else
                {
                    loadListener.onFatalError.accept("Encountered errors during load " + loadErrors);
                }
            }
        }

        void loading(long currentBucketStart, LoadListener loadListener)
        {
            loadingKeys.add(currentBucketStart);
            this.loadListener = loadListener;
        }

        void loadingStarted(long bucketCount)
        {
            this.bucketsLoading = bucketCount;
        }

        <U, F> void launchNewIteration(
            long requiredBucketCount,
            String installingClass,
            String definitionName,
            ReductionDefinition<T, U, F> reductionDefinition,
            IterationListener iterationListener,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            Optional<ByteBuffer> filterArgs,
            Collection<TimeCacheAgent> agents)
        {

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
            for (TimeCacheAgent agent : agents)
            {
                filterArgs.ifPresent(Buffer::mark);

                try
                {
                    agent.iterate(
                        definition.cacheName,
                        newIterationKey,
                        from,
                        toExclusive,
                        installingClass,
                        definitionName,
                        filterArgs);
                }
                finally
                {
                    filterArgs.ifPresent(Buffer::reset);
                }
            }
            iterationKey++;
        }

        void bucketComplete(String agentId, long iterationKey, long currentBucketKey, ByteBuffer result)
        {
            iterations
                .get(iterationKey)
                .bucketComplete(agentId, currentBucketKey, result);
        }
    }

    private static class IterationStatus<T, U, F>
    {
        private final long iterationKey;
        private long requiredBuckets;
        private final U accumulator;
        private final ReductionDefinition<T, U, F> reductionDefinition;
        private final IterationListener iterationListener;

        private IterationStatus(
            long iterationKey,
            long requiredBuckets,
            U accumulator,
            ReductionDefinition<T, U, F> reductionDefinition,
            IterationListener iterationListener)
        {
            this.iterationKey = iterationKey;
            this.requiredBuckets = requiredBuckets;
            this.accumulator = accumulator;
            this.reductionDefinition = reductionDefinition;
            this.iterationListener = iterationListener;
        }

        void bucketComplete(String agentId, long currentBucketKey, ByteBuffer result)
        {
            final U u = reductionDefinition.serializer.decode(new ReadableByteBuffer(result));
            reductionDefinition.reduceMany.accept(this.accumulator, u);
            requiredBuckets--;

            if (requiredBuckets == 0)
            {
                EmbiggenableBuffer buffer = EmbiggenableBuffer.allocate(128);
                reductionDefinition.serializer.encode(accumulator, buffer);
                iterationListener.onComplete.accept(new ReadableByteBuffer(buffer.asReadableByteBuffer()));
            }
        }
    }

    @Override
    public void load(
        String cacheName,
        ZonedDateTime fromInclusive,
        ZonedDateTime toExclusive,
        LoadListener loadListener)
    {
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
        for (int i = 0; i < bucketCount; i++)
        {
            distributedCacheStatus.loading(currentBucketStart, loadListener);
            TimeCacheAgent[] values = agents.values().toArray(new TimeCacheAgent[agents.size()]);
            values[i % agents.size()]
                .populateBucket(
                    distributedCacheStatus.definition.cacheName,
                    currentBucketStart,
                    currentBucketEnd);
            currentBucketStart = currentBucketEnd;
            currentBucketEnd += bucketSizeMillis;
        }
    }

    @Override
    public void iterate(
        String cacheName,
        ZonedDateTime from,
        ZonedDateTime toExclusive,
        String definingClass,
        String iterateeName,
        Optional<ByteBuffer> filterArguments,
        IterationListener iterationListener)
    {
        DistributedCacheStatus distributedCacheStatus = caches.get(cacheName);
        if (distributedCacheStatus == null)
        {
            iterationListener.onFatalError.accept(String.format("Cache '%s' not found", cacheName));
            return;
        }

        final ReductionDefinition reductionDefinition =
            definitions.get(definingClass).get(iterateeName);

        final long fromMillis = from.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final long bucketMillis = distributedCacheStatus.definition.bucketSize.toMillis(1L);
        final long firstBucketKey = (fromMillis / bucketMillis) * bucketMillis;
        final long lastBucketKey = (toMillis / bucketMillis) * bucketMillis;
        final long requiredBucketCount =
            (lastBucketKey - firstBucketKey) / bucketMillis + (firstBucketKey == lastBucketKey ? 1 : 0);

        //noinspection unchecked
        distributedCacheStatus.launchNewIteration(
            requiredBucketCount,
            definingClass,
            iterateeName,
            reductionDefinition,
            iterationListener,
            from,
            toExclusive,
            filterArguments,
            agents.values());
    }

    @Override
    public void loadComplete(
        String agentId,
        String cacheName,
        long bucketStart,
        long bucketEnd)
    {
        caches
            .get(cacheName)
            .loadComplete(agentId, bucketStart);
        timeCacheEvents.loadComplete(agentId, cacheName, bucketStart, bucketEnd);
    }

    @Override
    public void loadFailure(String agentId, String cacheName, long bucketStart, String message)
    {
        caches
            .get(cacheName)
            .loadFailure(agentId, bucketStart, message);
    }

    @Override
    public void bucketComplete(
        String agentId,
        String cacheName,
        long iterationKey,
        long currentBucketKey,
        ByteBuffer result)
    {
        timeCacheEvents
            .iterationBucketComplete(
                agentId, cacheName, iterationKey, currentBucketKey);
        caches
            .get(cacheName)
            .bucketComplete(agentId, iterationKey, currentBucketKey, result);
    }

    @Override
    public void installationComplete(String agentName, String installationKlass)
    {
        installationProgress.get(installationKlass).complete(agentName);
        timeCacheEvents.definitionsInstalled(agentName, installationKlass);
    }

    @Override
    public void installationError(
        String agentName,
        String installationKlass,
        String errorMessage)
    {
        installationProgress.get(installationKlass).error(agentName, errorMessage);
    }

    @Override
    public void cacheDefined(String agentId, String cacheName)
    {
        definitionStatuses.get(cacheName).agentDefinitionComplete(agentId);
    }

    @Override
    public void cacheDefinitionFailed(String agentId, String cacheName, String errorMessage)
    {
        definitionStatuses.get(cacheName).agentDefinitionFailed(agentId, errorMessage);
    }

    public void addAgent(String agentName, TimeCacheAgent timeCacheAgent)
    {
        agents.put(agentName, timeCacheAgent);
        timeCacheEvents.onAgentConnected();
    }

    @Override
    public void defineCache(
        String cacheName,
        String cacheComponentFactoryClass,
        DefinitionListener definitionListener)
    {
        Result<CacheComponentsFactory, Exception> result =
            ClassLoading.loadAndCast(cacheComponentFactoryClass, CacheComponentsFactory.class);
        result.consume(
            cacheComponentsFactory -> {
                CacheComponentsFactory.CacheComponents<?> cacheComponents = cacheComponentsFactory.createCacheComponents();
                //noinspection unchecked
                definitionStatuses
                    .put(
                        cacheName,
                        new DefinitionStatus(
                            cacheName,
                            cacheComponents,
                            definitionListener,
                            new HashSet<>(agents.keySet())
                        ));

                agents
                    .values()
                    .forEach(agent -> agent.defineCache(cacheName, cacheComponentFactoryClass));
            },
            exception ->
                definitionListener.onError.accept(
                    "Unable to define cache '" + cacheName + "', encountered " + exception.toString())
        );
    }

    private final class DefinitionStatus
    {
        private final String cacheName;
        private final CacheComponentsFactory.CacheComponents<?> cacheComponents;
        private final DefinitionListener definitionListener;
        private final HashSet<String> waitingFor;
        private final List<String> errors = new ArrayList<>();

        private DefinitionStatus(
            String cacheName,
            CacheComponentsFactory.CacheComponents<?> cacheComponents,
            DefinitionListener definitionListener,
            HashSet<String> waitingFor)
        {
            this.cacheName = cacheName;
            this.cacheComponents = cacheComponents;
            this.definitionListener = definitionListener;
            this.waitingFor = waitingFor;
        }

        void agentDefinitionComplete(String agentId)
        {
            waitingFor.remove(agentId);
            checkCompletion();
        }

        public void agentDefinitionFailed(String agentId, String errorMessage)
        {
            waitingFor.remove(agentId);
            errors.add(
                "Agent " + agentId + " failed to load cache definition for cache " + cacheName + " due to " +
                    errorMessage);
            checkCompletion();
        }

        private void checkCompletion()
        {
            if (waitingFor.isEmpty())
            {
                if (errors.isEmpty())
                {

                    //noinspection unchecked
                    caches.put(
                        cacheName,
                        new DistributedCacheStatus(
                            new CacheDefinition(
                                cacheName,
                                cacheComponents.cacheClass,
                                cacheComponents.eventLoader,
                                cacheComponents.millitimeExtractor,
                                cacheComponents.bucketSize,
                                cacheComponents.serializer),
                            new HashSet<>(),
                            new HashMap<>()
                        ));
                    definitionListener.onSuccess.run();
                }
                else
                {
                    definitionListener.onError.accept("Failed to define cache due to " + errors);
                }
            }
        }
    }
}
