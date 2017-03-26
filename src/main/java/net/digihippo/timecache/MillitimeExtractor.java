package net.digihippo.timecache;

interface MillitimeExtractor<T>
{
    long apply(final T t);
}
