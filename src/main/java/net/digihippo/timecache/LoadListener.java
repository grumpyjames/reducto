package net.digihippo.timecache;

import java.util.function.Consumer;

public final class LoadListener
{
    final Runnable onComplete;
    final Consumer<String> onFatalError;

    public LoadListener(
        Runnable onComplete,
        Consumer<String> onFatalError)
    {
        this.onComplete = onComplete;
        this.onFatalError = onFatalError;
    }
}
