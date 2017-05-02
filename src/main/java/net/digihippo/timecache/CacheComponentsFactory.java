package net.digihippo.timecache;

import java.util.concurrent.TimeUnit;

public interface CacheComponentsFactory<T>
{
    final class CacheComponents<T>
    {
        final Class<T> cacheClass;
        final Serializer<T> serializer;
        final EventLoader<T> eventLoader;
        final MillitimeExtractor<T> millitimeExtractor;
        final TimeUnit bucketSize;

        public CacheComponents(
            Class<T> cacheClass,
            Serializer<T> serializer,
            EventLoader<T> eventLoader,
            MillitimeExtractor<T> millitimeExtractor,
            TimeUnit bucketSize)
        {
            this.cacheClass = cacheClass;
            this.serializer = serializer;
            this.eventLoader = eventLoader;
            this.millitimeExtractor = millitimeExtractor;
            this.bucketSize = bucketSize;
        }
    }

    CacheComponents<T> createCacheComponents();
}
