package net.digihippo.timecache;

public interface TimeCacheServer {
    void loadComplete(
            String agentId,
            TimeCache.CacheDefinition<?> cacheDefinition,
            long bucketStart,
            long bucketEnd);
}
