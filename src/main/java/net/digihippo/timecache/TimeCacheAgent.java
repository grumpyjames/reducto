package net.digihippo.timecache;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Optional;

public interface TimeCacheAgent {
    void installDefinitions(String className);

    void populateBucket(
        String cacheName,
        long currentBucketStart,
        long currentBucketEnd);

    void iterate(
        String cacheName,
        long iterationKey,
        ZonedDateTime from,
        ZonedDateTime toExclusive,
        String installingClass,
        String definitionName, Optional<ByteBuffer> wireFilterArgs);

    void defineCache(
        String cacheName,
        String cacheComponentFactoryClass);
}
