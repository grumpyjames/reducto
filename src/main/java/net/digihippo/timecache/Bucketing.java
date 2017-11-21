package net.digihippo.timecache;

import java.time.ZonedDateTime;

final class Bucketing
{
    static class Buckets
    {
        final long firstBucketKey;
        final long bucketCount;

        Buckets(long firstBucketKey, long bucketCount)
        {
            this.firstBucketKey = firstBucketKey;
            this.bucketCount = bucketCount;
        }
    }

    static Buckets calculateBuckets(
        ZonedDateTime fromInclusive,
        ZonedDateTime toExclusive,
        long bucketSizeMillis)
    {
        final long fromMillis = fromInclusive.toInstant().toEpochMilli();
        final long toMillis = toExclusive.toInstant().toEpochMilli();
        final long firstBucketKey = (fromMillis / bucketSizeMillis) * bucketSizeMillis;
        final long remainder = (toMillis - firstBucketKey) % bucketSizeMillis;
        long requiredBucketCount = (toMillis - firstBucketKey) / bucketSizeMillis;
        if (remainder != 0)
        {
            requiredBucketCount = 1 + ((toMillis - firstBucketKey) / bucketSizeMillis);
        }

        return new Buckets(firstBucketKey, requiredBucketCount);
    }

    static long upToMultiple(final long bucketSize, final long epochMilli)
    {
        long remainder = epochMilli % bucketSize;
        if (remainder == 0)
        {
            return epochMilli;
        }
        else
        {
            return epochMilli - remainder + bucketSize;
        }
    }

    private Bucketing() {}
}
