package net.digihippo.timecache;

import net.digihippo.timecache.api.*;

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
        TimeCacheAgent[] agents = this.agents.values().toArray(new TimeCacheAgent[this.agents.size()]);

        distributedCacheStatus.load(fromInclusive, toExclusive, loadListener, agents);
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
        Collection<TimeCacheAgent> agents = this.agents.values();

        distributedCacheStatus.iterate(from, toExclusive, definingClass, iterateeName, filterArguments, iterationListener, reductionDefinition, agents);
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
                CacheComponentsFactory.CacheComponents<?> cacheComponents =
                    cacheComponentsFactory.createCacheComponents();
                //noinspection unchecked
                definitionStatuses
                    .put(
                        cacheName,
                        new DefinitionStatus(
                            cacheName,
                            cacheComponents,
                            new DefinitionCallback((k, v) -> {
                                this.caches.put(k, v);
                                definitionListener.onSuccess.run();
                            }, definitionListener.onError),
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

}
