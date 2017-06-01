package net.digihippo.timecache;

import java.util.Map;
import java.util.function.Consumer;

public final class InstallationListener
{
    public final Runnable onComplete;
    public final Consumer<Map<String, String>> onError;

    public InstallationListener(
        Runnable onComplete,
        Consumer<Map<String, String>> onError)
    {
        this.onComplete = onComplete;
        this.onError = onError;
    }
}
