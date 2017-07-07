package net.digihippo.timecache;

import net.digihippo.timecache.api.EventLoader;
import net.digihippo.timecache.api.MillitimeExtractor;
import net.digihippo.timecache.api.Serializer;

import java.util.concurrent.TimeUnit;

final class CacheDefinition<T>
{
    final String cacheName;
    final Class<T> cacheClass;
    final EventLoader<T> eventLoader;
    final MillitimeExtractor<T> millitimeExtractor;
    final TimeUnit bucketSize;
    final Serializer<T> serializer;

    CacheDefinition(
        String cacheName,
        Class<T> cacheClass,
        EventLoader<T> eventLoader,
        MillitimeExtractor<T> millitimeExtractor,
        TimeUnit bucketSize,
        Serializer<T> serializer)
    {
        this.cacheName = cacheName;
        this.cacheClass = cacheClass;
        this.eventLoader = eventLoader;
        this.millitimeExtractor = millitimeExtractor;
        this.bucketSize = bucketSize;
        this.serializer = serializer;
    }
}
