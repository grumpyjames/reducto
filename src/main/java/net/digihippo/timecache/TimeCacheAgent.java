package net.digihippo.timecache;

import java.time.ZonedDateTime;

interface TimeCacheAgent {
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
        String definitionName);

    void defineCache(String cacheName, String cacheComponentFactoryClass);
}
