package net.digihippo.timecache;

import java.util.function.Consumer;

public class DefinitionListener
{
    public final Consumer<String> onError;
    public final Runnable onSuccess;

    public DefinitionListener(Consumer<String> onError, Runnable onSuccess)
    {
        this.onError = onError;
        this.onSuccess = onSuccess;
    }
}
