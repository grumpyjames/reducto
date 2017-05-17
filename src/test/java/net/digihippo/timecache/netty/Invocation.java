package net.digihippo.timecache.netty;

import org.jmock.Mockery;

interface Invocation<T>
{
    void installExpectation(final Mockery mockery, final T mock);
    void run(final T agent);
}
