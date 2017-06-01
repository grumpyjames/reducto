package net.digihippo.timecache;

import java.util.function.Consumer;

public final class LoadListener
{
    public final Runnable onComplete;
    public final Consumer<String> onFatalError;

    public LoadListener(
        Runnable onComplete,
        Consumer<String> onFatalError)
    {
        this.onComplete = onComplete;
        this.onFatalError = onFatalError;
    }
}
