package net.digihippo.timecache;

import java.nio.ByteBuffer;

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
}
