package net.digihippo.timecache.api;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class FilterDefinition<F, T>
{
    public final Serializer<F> filterSerializer;
    public final Function<Optional<F>, Predicate<T>> predicateLoader;

    public FilterDefinition(Serializer<F> filterSerializer, Function<Optional<F>, Predicate<T>> predicateLoader)
    {
        this.filterSerializer = filterSerializer;
        this.predicateLoader = predicateLoader;
    }
}
