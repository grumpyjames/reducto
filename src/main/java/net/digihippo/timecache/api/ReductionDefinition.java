package net.digihippo.timecache.api;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class ReductionDefinition<T, U, F>
{
    public final Supplier<U> initialSupplier;
    public final BiConsumer<U, T> reduceOne;
    public final BiConsumer<U, U> reduceMany;
    public final Serializer<U> serializer;
    public final FilterDefinition<F, T> filterDefinition;

    public ReductionDefinition(
        Supplier<U> initialSupplier,
        BiConsumer<U, T> reduceOne,
        BiConsumer<U, U> reduceMany,
        Serializer<U> serializer,
        FilterDefinition<F, T> filterDefinition)
    {
        this.initialSupplier = initialSupplier;
        this.reduceOne = reduceOne;
        this.reduceMany = reduceMany;
        this.serializer = serializer;
        this.filterDefinition = filterDefinition;
    }
}
