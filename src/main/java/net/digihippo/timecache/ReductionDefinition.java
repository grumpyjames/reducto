package net.digihippo.timecache;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class ReductionDefinition<T, U>
{
    public final Supplier<U> initialSupplier;
    public final BiConsumer<U, T> reduceOne;
    public final BiConsumer<U, U> reduceMany;
    public final Serializer<U> serializer;

    public ReductionDefinition(
        Supplier<U> initialSupplier,
        BiConsumer<U, T> reduceOne,
        BiConsumer<U, U> reduceMany,
        Serializer<U> serializer)
    {
        this.initialSupplier = initialSupplier;
        this.reduceOne = reduceOne;
        this.reduceMany = reduceMany;
        this.serializer = serializer;
    }
}
