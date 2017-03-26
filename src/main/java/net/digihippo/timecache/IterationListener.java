package net.digihippo.timecache;

import java.util.function.Consumer;

public class IterationListener<U> {
    public final Consumer<U> onComplete;
    public final Consumer<String> onFatalError;

    public IterationListener(
            Consumer<U> onComplete,
            Consumer<String> onFatalError) {
        this.onComplete = onComplete;
        this.onFatalError = onFatalError;
    }
}
