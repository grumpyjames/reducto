package net.digihippo.timecache;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;

public class InMemoryTimeCacheAgent implements TimeCacheAgent
{
    private final String agentId;
    private final TimeCacheServer timeCacheServer;
    private final Map<String, Cache<?>> caches = new HashMap<>();

    public InMemoryTimeCacheAgent(
        String agentId,
        TimeCacheServer timeCacheServer)
    {
        this.agentId = agentId;
        this.timeCacheServer = timeCacheServer;
    }

    private final Map<String, Map<String, ReductionDefinition<?, ?>>> definitions = new HashMap<>();

    @Override
    public void installDefinitions(String name)
    {
        Result<DefinitionSource, Exception> result = ClassLoading.loadAndCast(name, DefinitionSource.class);
        result.consume(
            definitionSource -> this.definitions.put(name, definitionSource.definitions()),
            exc -> {
                throw new RuntimeException(exc);
            }
        );
    }

    @Override
    public void populateBucket(
        String cacheName,
        long currentBucketStart,
        long currentBucketEnd)
    {
        Cache cache = caches.get(cacheName);
        //noinspection unchecked
        cache
            .loadEvents(
                currentBucketStart,
                currentBucketEnd);

        timeCacheServer
            .loadComplete(
                agentId,
                cacheName,
                currentBucketStart,
                currentBucketEnd);
    }

    @Override
    public void iterate(
        String cacheName,
        long iterationKey,
        ZonedDateTime from,
        ZonedDateTime toExclusive,
        String installerName,
        String definitionName)
    {

        ReductionDefinition definition =
            definitions.get(installerName).get(definitionName);
        Cache cache = caches.get(cacheName);
        if (cache != null)
        {
            //noinspection unchecked
            cache.iterate(
                agentId,
                cacheName,
                iterationKey,
                from,
                toExclusive,
                definition,
                timeCacheServer);
        }
    }

    @Override
    public void defineCache(
        String cacheName,
        String cacheComponentFactoryClass)
    {
        Result<CacheComponentsFactory, Exception> result =
            ClassLoading.loadAndCast(cacheComponentFactoryClass, CacheComponentsFactory.class);
        result.consume(
            ccf -> {
                CacheComponentsFactory.CacheComponents cacheComponents = ccf.createCacheComponents();
                @SuppressWarnings("unchecked") final TimeCache.CacheDefinition definition =
                    new TimeCache.CacheDefinition(
                        cacheName,
                        cacheComponents.cacheClass,
                        cacheComponents.eventLoader,
                        cacheComponents.millitimeExtractor,
                        cacheComponents.bucketSize);
                //noinspection unchecked
                caches.put(cacheName, new Cache(definition, cacheComponents.bucketSize.toMillis(1L)));
            },
            exc -> {
                throw new RuntimeException(exc);
            }
        );
    }

    public static class Cache<T>
    {
        private final TimeCache.CacheDefinition<T> cacheDefinition;
        private final long bucketSize;
        private final Map<Long, List<T>> buckets = new HashMap<>();

        public Cache(
            TimeCache.CacheDefinition<T> cacheDefinition,
            long bucketSizeMillis)
        {
            this.cacheDefinition = cacheDefinition;
            this.bucketSize = bucketSizeMillis;
        }

        public Consumer<T> newBucket(long bucketStart)
        {
            List list = buckets.computeIfAbsent(bucketStart, bs -> new ArrayList());
            //noinspection unchecked
            return list::add;
        }

        public <U> void iterate(
            String agentId,
            String cacheName,
            long iterationKey,
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
                                    item -> definition.reduceOne.accept(result, item));
                            server.bucketComplete(agentId, cacheName, iterationKey, currentBucketKey, result);
                        });
                bucketKey += bucketSize;
            }
        }

        public void loadEvents(
            long from,
            long toExclusive)
        {
            cacheDefinition.eventLoader.loadEvents(
                Instant.ofEpochMilli(from),
                Instant.ofEpochMilli(toExclusive),
                newBucket(from));
        }
    }
}
