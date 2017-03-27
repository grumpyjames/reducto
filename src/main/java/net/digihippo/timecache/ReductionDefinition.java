package net.digihippo.timecache;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class ReductionDefinition<T, U> {
    public final Supplier<U> initialSupplier;
    public final BiConsumer<T, U> reduceOne;
    public final BiConsumer<U, U> reduceMany;

    public ReductionDefinition(
        Supplier<U> initialSupplier,
        BiConsumer<T, U> reduceOne,
        BiConsumer<U, U> reduceMany) {
        this.initialSupplier = initialSupplier;
        this.reduceOne = reduceOne;
        this.reduceMany = reduceMany;
    }
}