package net.digihippo.timecache.api;

public interface MillitimeExtractor<T>
{
    long apply(final T t);
}
