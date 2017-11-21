package net.digihippo.timecache;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class DefinitionCallback
{
    final BiConsumer<String, DistributedCacheStatus> onSuccess;
    final Consumer<String> onError;

    DefinitionCallback(
        BiConsumer<String, DistributedCacheStatus> onSuccess,
        Consumer<String> onError)
    {
        this.onSuccess = onSuccess;
        this.onError = onError;
    }
}
