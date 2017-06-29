package net.digihippo.timecache;

import net.digihippo.timecache.api.ReadBuffer;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public final class IterationListener
{
    public final Consumer<ReadBuffer> onComplete;
    public final Consumer<String> onFatalError;

    public IterationListener(
        Consumer<ReadBuffer> onComplete,
        Consumer<String> onFatalError)
    {
        this.onComplete = onComplete;
        this.onFatalError = onFatalError;
    }
}
