package net.digihippo.timecache.netty;

interface Invocation<T>
{
    void run(final T t);
}
