package net.digihippo.timecache;

public interface TimeCacheServer {
    void loadComplete(
        String agentId,
        String cacheName,
        long bucketStart,
        long bucketEnd);

    void bucketComplete(
        String agentId,
        String cacheName,
        long iterationKey,
        long currentBucketKey,
        Object result);

    void installationComplete(
        String agentName,
        String installationKlass);

    void installationError(
        String agentName,
        String installationKlass,
        String errorMessage);
}
