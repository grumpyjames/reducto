package net.digihippo.timecache;

import java.util.function.Consumer;

public class DefinitionListener
{
    public final Consumer<String> onError;
    public final Runnable onSuccess;

    public DefinitionListener(
        Runnable onSuccess,
        Consumer<String> onError)
    {
        this.onError = onError;
        this.onSuccess = onSuccess;
    }
}
