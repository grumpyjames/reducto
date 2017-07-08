package net.digihippo.timecache;

import net.digihippo.timecache.api.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class InMemoryTimeCacheAgent implements TimeCacheAgent
{
    private final File rootDir;
    private final String agentId;
    private final TimeCacheServer timeCacheServer;
    private final Map<String, Cache<?>> caches = new HashMap<>();

    public InMemoryTimeCacheAgent(
        File rootDir,
        String agentId,
        TimeCacheServer timeCacheServer)
    {
        this.rootDir = rootDir;
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
        try
        {
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
        catch (Exception e)
        {
            timeCacheServer.loadFailure(agentId, cacheName, currentBucketStart, e.getMessage());
        }
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
                wireFilterArgs
                    .map(ReadableByteBuffer::new)
                    .map((Function<ReadBuffer, Object>) definition.filterDefinition.filterSerializer::decode));


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
                final File cacheDirectory = new File(rootDir, cacheName);
                if (!cacheDirectory.mkdir())
                {
                    timeCacheServer.cacheDefinitionFailed(
                        agentId,
                        cacheName,
                        "Unable to create file: " + cacheDirectory.getAbsolutePath());
                    return;
                }
                @SuppressWarnings("unchecked") final CacheDefinition definition =
                    new CacheDefinition(
                        cacheName,
                        cacheComponents.cacheClass,
                        cacheComponents.eventLoader,
                        cacheComponents.millitimeExtractor,
                        cacheComponents.bucketSize,
                        cacheComponents.serializer);
                //noinspection unchecked
                caches.put(cacheName, new Cache(definition, cacheDirectory, cacheComponents.bucketSize.toMillis(1L)));
                timeCacheServer.cacheDefined(agentId, cacheName);
            },
            exc -> timeCacheServer.cacheDefinitionFailed(agentId, cacheName, exc.getMessage())
        );
    }

    private static class Cache<T>
    {
        private final CacheDefinition<T> cacheDefinition;
        private final File cacheDirectory;
        private final long bucketSize;
        private final Map<Long, Bucket<T>> buckets = new HashMap<>();

        Cache(
            CacheDefinition<T> cacheDefinition,
            File cacheDirectory,
            long bucketSizeMillis)
        {
            this.cacheDefinition = cacheDefinition;
            this.cacheDirectory = cacheDirectory;
            this.bucketSize = bucketSizeMillis;
        }

        Consumer<T> newBucket(long bucketStart)
        {
            Bucket<T> bucket = buckets.computeIfAbsent(
                bucketStart,
                bs -> createBucket(cacheDirectory, bucketStart, cacheDefinition.serializer));
            //noinspection unchecked
            return bucket::add;
        }

        private Bucket<T> createBucket(
            File cacheDirectory,
            long bucketStart,
            Serializer<T> serializer)
        {
            final File bucketFile = new File(cacheDirectory, Long.toString(bucketStart));
            try
            {
                final RandomAccessFile raf = new RandomAccessFile(bucketFile, "rw");
                return new Bucket<>(new MemoryMappedBuffer(raf), serializer);
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }
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

                            final EmbiggenableBuffer buffer = EmbiggenableBuffer.allocate(128);
                            definition.serializer.encode(result, buffer);
                            ByteBuffer buf = buffer.asReadableByteBuffer();

                            server.bucketComplete(agentId, cacheName, iterationKey, currentBucketKey, buf);
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

    private static class Bucket<T>
    {
        private final MemoryMappedBuffer memoryMappedBuffer;
        private final Serializer<T> serializer;

        public Bucket(
            MemoryMappedBuffer memoryMappedBuffer,
            Serializer<T> serializer)
        {
            this.memoryMappedBuffer = memoryMappedBuffer;
            this.serializer = serializer;
        }

        public Stream<T> stream()
        {
            memoryMappedBuffer.prepareForRead();

            return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(new Iterator<T>() {
                    @Override
                    public boolean hasNext()
                    {
                        return memoryMappedBuffer.hasBytes();
                    }

                    @Override
                    public T next()
                    {
                        return serializer.decode(memoryMappedBuffer);
                    }
                }, Spliterator.ORDERED), false);
        }

        public void add(T t)
        {
            serializer.encode(t, memoryMappedBuffer);
        }
    }
}
