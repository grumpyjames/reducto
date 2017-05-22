package net.digihippo.timecache;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public final class IterationListener
{
    public final Consumer<ByteBuffer> onComplete;
    public final Consumer<String> onFatalError;

    public IterationListener(
        Consumer<ByteBuffer> onComplete,
        Consumer<String> onFatalError)
    {
        this.onComplete = onComplete;
        this.onFatalError = onFatalError;
    }
}
