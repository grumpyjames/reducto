package net.digihippo.timecache;

public interface TimeCacheServer {
    void loadComplete(
            String agentId,
            TimeCache.CacheDefinition<?> cacheDefinition,
            long bucketStart,
            long bucketEnd);

    void bucketComplete(
            String agentId,
            long currentBucketKey,
            Object result);
}
