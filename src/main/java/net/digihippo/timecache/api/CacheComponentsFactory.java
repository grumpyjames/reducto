package net.digihippo.timecache.api;

import java.util.concurrent.TimeUnit;

public interface CacheComponentsFactory<T>
{
    final class CacheComponents<T>
    {
        public final Class<T> cacheClass;
        public final Serializer<T> serializer;
        public final EventLoader<T> eventLoader;
        public final MillitimeExtractor<T> millitimeExtractor;
        public final TimeUnit bucketSize;

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
