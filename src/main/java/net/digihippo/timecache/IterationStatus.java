package net.digihippo.timecache;

import net.digihippo.timecache.api.ReductionDefinition;

import java.nio.ByteBuffer;

class IterationStatus<T, U, F>
{
    private final long iterationKey;
    private long requiredBuckets;
    private final U accumulator;
    private final ReductionDefinition<T, U, F> reductionDefinition;
    private final IterationListener iterationListener;

    IterationStatus(
        long iterationKey,
        long requiredBuckets,
        U accumulator,
        ReductionDefinition<T, U, F> reductionDefinition,
        IterationListener iterationListener)
    {
        this.iterationKey = iterationKey;
        this.requiredBuckets = requiredBuckets;
        this.accumulator = accumulator;
        this.reductionDefinition = reductionDefinition;
        this.iterationListener = iterationListener;
    }

    void bucketComplete(String agentId, long currentBucketKey, ByteBuffer result)
    {
        final U u = reductionDefinition.serializer.decode(new ReadableByteBuffer(result));
        reductionDefinition.reduceMany.accept(this.accumulator, u);
        requiredBuckets--;

        if (requiredBuckets == 0)
        {
            EmbiggenableBuffer buffer = EmbiggenableBuffer.allocate(128);
            reductionDefinition.serializer.encode(accumulator, buffer);
            iterationListener.onComplete.accept(new ReadableByteBuffer(buffer.asReadableByteBuffer()));
        }
    }
}
