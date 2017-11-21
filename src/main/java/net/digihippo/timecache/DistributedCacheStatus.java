package net.digihippo.timecache;

import net.digihippo.timecache.api.ReductionDefinition;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.*;

final class DistributedCacheStatus<T>
{
    private final CacheDefinition<T> definition;
    private final Set<Long> loadingKeys;
    private final Map<Long, Set<String>> bucketOwners;
    private final Map<Long, IterationStatus> iterations = new HashMap<>();

    private long iterationKey;
    private LoadListener loadListener;
    private long bucketsLoading;
    private final List<String> loadErrors = new ArrayList<>();

    DistributedCacheStatus(
        CacheDefinition<T> definition,
        Set<Long> loadingKeys,
        Map<Long, Set<String>> bucketOwners)
    {
        this.definition = definition;
        this.loadingKeys = loadingKeys;
        this.bucketOwners = bucketOwners;
    }

    public void loadComplete(String agentId, long bucketStart)
    {
        loadingKeys.remove(bucketStart);
        --bucketsLoading;
        bucketOwners.computeIfAbsent(bucketStart, (b) -> new HashSet<>()).add(agentId);
        checkCompletion();
    }

    void loadFailure(String agentId, long bucketStart, String message)
    {
        loadingKeys.remove(bucketStart);
        --bucketsLoading;
        loadErrors.add("Agent " + agentId + " failed to load bucket " + bucketStart + " due to " + message);
        checkCompletion();
    }

    private void checkCompletion()
    {
        if (bucketsLoading == 0)
        {
            if (loadErrors.isEmpty())
            {
                loadListener.onComplete.run();
            }
            else
            {
                loadListener.onFatalError.accept("Encountered errors during load " + loadErrors);
            }
        }
    }

    void loadingStarted(long bucketCount, LoadListener loadListener)
    {
        this.bucketsLoading = bucketCount;
        this.loadListener = loadListener;
    }

    void bucketComplete(String agentId, long iterationKey, long currentBucketKey, ByteBuffer result)
    {
        iterations
            .get(iterationKey)
            .bucketComplete(agentId, currentBucketKey, result);
    }

    void load(
        ZonedDateTime fromInclusive,
        ZonedDateTime toExclusive,
        LoadListener loadListener,
        TimeCacheAgent[] agents)
    {
        final long fromMillis = fromInclusive.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final long bucketSizeMillis = definition.bucketSize.toMillis(1L);

        final long firstBucketKey = (fromMillis / bucketSizeMillis) * bucketSizeMillis;
        final long maybeLastBucketKey = (toMillis / bucketSizeMillis * bucketSizeMillis);
        final long lastBucketKey = maybeLastBucketKey == toMillis ? toMillis - bucketSizeMillis : maybeLastBucketKey;
        final long bucketCount = 1 + (lastBucketKey - firstBucketKey) / bucketSizeMillis;

        long currentBucketStart = firstBucketKey;
        long currentBucketEnd = currentBucketStart + bucketSizeMillis;
        loadingStarted(bucketCount, loadListener);
        for (int i = 0; i < bucketCount; i++)
        {
            loadingKeys.add(currentBucketStart);
            agents[i % agents.length]
                .populateBucket(
                    definition.cacheName,
                    currentBucketStart,
                    currentBucketEnd);
            currentBucketStart = currentBucketEnd;
            currentBucketEnd += bucketSizeMillis;
        }
    }

    void iterate(
        ZonedDateTime from,
        ZonedDateTime toExclusive,
        String definingClass,
        String iterateeName,
        Optional<ByteBuffer> filterArguments,
        IterationListener iterationListener,
        ReductionDefinition reductionDefinition,
        Collection<TimeCacheAgent> agents)
    {
        final long fromMillis = from.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final long bucketMillis = definition.bucketSize.toMillis(1L);
        final long firstBucketKey = (fromMillis / bucketMillis) * bucketMillis;
        final long lastBucketKey = (toMillis / bucketMillis) * bucketMillis;
        final long requiredBucketCount =
            (lastBucketKey - firstBucketKey) / bucketMillis + (firstBucketKey == lastBucketKey ? 1 : 0);

        long newIterationKey = iterationKey;
        //noinspection unchecked
        iterations.put(
            newIterationKey,
            new IterationStatus<>(
                newIterationKey,
                requiredBucketCount,
                reductionDefinition.initialSupplier.get(),
                reductionDefinition,
                iterationListener
            ));
        for (TimeCacheAgent agent : agents)
        {
            filterArguments.ifPresent(Buffer::mark);

            try
            {
                agent.iterate(
                    definition.cacheName,
                    newIterationKey,
                    from,
                    toExclusive,
                    definingClass,
                    iterateeName,
                    filterArguments);
            }
            finally
            {
                filterArguments.ifPresent(Buffer::reset);
            }
        }
        iterationKey++;
    }
}
