package net.digihippo.timecache;

import java.nio.ByteBuffer;

public interface TimeCacheServer {
    void loadComplete(
        String agentId,
        String cacheName,
        long bucketStart,
        long bucketEnd);

    void loadFailure(
        String agentId,
        String cacheName,
        long currentBucketStart,
        String message);

    void bucketComplete(
        String agentId,
        String cacheName,
        long iterationKey,
        long currentBucketKey,
        ByteBuffer result);

    void installationComplete(
        String agentName,
        String installationKlass);

    void installationError(
        String agentName,
        String installationKlass,
        String errorMessage);

    void cacheDefined(
        String agentId,
        String cacheName);

    void cacheDefinitionFailed(
        String agentId,
        String cacheName,
        String errorMessage);
}
