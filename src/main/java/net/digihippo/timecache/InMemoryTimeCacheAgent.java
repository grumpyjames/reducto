package net.digihippo.timecache;

import net.digihippo.timecache.api.CacheComponentsFactory;
import net.digihippo.timecache.api.DefinitionSource;
import net.digihippo.timecache.api.ReductionDefinition;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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

    private final Map<String, Map<String, ReductionDefinition<?, ?, ?>>> definitions = new HashMap<>();

    @Override
    public void installDefinitions(String name)
    {
        Result<DefinitionSource, Exception> result = ClassLoading.loadAndCast(name, DefinitionSource.class);
        result.consume(
            definitionSource -> {
                definitions.put(name, definitionSource.definitions());
                timeCacheServer.installationComplete(agentId, name);
            },
            exc -> timeCacheServer.installationError(agentId, name, exc.getMessage())
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
        String definitionName,
        Optional<ByteBuffer> wireFilterArgs)
    {

        ReductionDefinition definition =
            definitions.get(installerName).get(definitionName);
        @SuppressWarnings("unchecked") final Predicate predicate =
            (Predicate) definition.filterDefinition.predicateLoader.apply(
                wireFilterArgs.map((Function<ByteBuffer, Object>) definition.filterDefinition.filterSerializer::decode));


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
                predicate,
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
                @SuppressWarnings("unchecked") final CacheDefinition definition =
                    new CacheDefinition(
                        cacheName,
                        cacheComponents.cacheClass,
                        cacheComponents.eventLoader,
                        cacheComponents.millitimeExtractor,
                        cacheComponents.bucketSize);
                //noinspection unchecked
                caches.put(cacheName, new Cache(definition, cacheComponents.bucketSize.toMillis(1L)));
                timeCacheServer.cacheDefined(agentId, cacheName);
            },
            exc -> {
                throw new RuntimeException(exc);
            }
        );
    }

    private static class Cache<T>
    {
        private final CacheDefinition<T> cacheDefinition;
        private final long bucketSize;
        private final Map<Long, List<T>> buckets = new HashMap<>();

        Cache(
                CacheDefinition<T> cacheDefinition,
                long bucketSizeMillis)
        {
            this.cacheDefinition = cacheDefinition;
            this.bucketSize = bucketSizeMillis;
        }

        Consumer<T> newBucket(long bucketStart)
        {
            List list = buckets.computeIfAbsent(bucketStart, bs -> new ArrayList());
            //noinspection unchecked
            return list::add;
        }

        public <U, F> void iterate(
            String agentId,
            String cacheName,
            long iterationKey,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            ReductionDefinition<T, U, F> definition,
            Predicate<T> predicate,
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
                                    return fromEpochMilli <= eTime && eTime < toEpochMilli && predicate.test(e);
                                })
                                .forEach(
                                    item -> definition.reduceOne.accept(result, item));

                            final ByteBuffer buffer = ByteBuffer.allocate(1024);
                            definition.serializer.encode(result, buffer);
                            buffer.flip();

                            server.bucketComplete(agentId, cacheName, iterationKey, currentBucketKey, buffer);
                        });
                bucketKey += bucketSize;
            }
        }

        void loadEvents(
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
