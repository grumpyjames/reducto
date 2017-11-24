package net.digihippo.timecache;

import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.*;

public class BucketingTest
{
    @Test
    public void startAndEndWithinOneBucket()
    {
        Bucketing.Buckets buckets = Bucketing.calculateBuckets(
            time(1200L),
            time(1213L),
            100L);

        assertEquals(1, buckets.bucketCount);
        assertEquals(1200, buckets.firstBucketKey);
    }

    @Test
    public void startAndEndInSeparateBucketsWithRespectToBucketBoundaryChoice()
    {
        Bucketing.Buckets buckets = Bucketing.calculateBuckets(
            time(1199L),
            time(1213L),
            100L);

        assertEquals(2, buckets.bucketCount);
        assertEquals(1100, buckets.firstBucketKey);
    }

    @Test
    public void endIsExclusive()
    {
        Bucketing.Buckets buckets = Bucketing.calculateBuckets(
            time(1199L),
            time(1300L),
            100L);

        assertEquals(2, buckets.bucketCount);
        assertEquals(1100, buckets.firstBucketKey);
    }

    private ZonedDateTime time(long instant)
    {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(instant), ZoneId.of("UTC"));
    }
}