package net.digihippo.timecache;

import net.digihippo.timecache.api.EventLoader;
import net.digihippo.timecache.api.MillitimeExtractor;

import java.util.concurrent.TimeUnit;

final class CacheDefinition<T>
{
    final String cacheName;
    final Class<T> cacheClass;
    final EventLoader<T> eventLoader;
    final MillitimeExtractor<T> millitimeExtractor;
    final TimeUnit bucketSize;

    CacheDefinition(
        String cacheName,
        Class<T> cacheClass,
        EventLoader<T> eventLoader,
        MillitimeExtractor<T> millitimeExtractor,
        TimeUnit bucketSize)
    {

        this.cacheName = cacheName;
        this.cacheClass = cacheClass;
        this.eventLoader = eventLoader;
        this.millitimeExtractor = millitimeExtractor;
        this.bucketSize = bucketSize;
    }
}
