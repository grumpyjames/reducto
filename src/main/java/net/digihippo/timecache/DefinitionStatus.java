package net.digihippo.timecache;

import net.digihippo.timecache.api.CacheComponentsFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

final class DefinitionStatus
{
    private final String cacheName;
    private final CacheComponentsFactory.CacheComponents<?> cacheComponents;
    private final DefinitionCallback definitionCallback;
    private final HashSet<String> waitingFor;
    private final List<String> errors = new ArrayList<>();

    DefinitionStatus(
        String cacheName,
        CacheComponentsFactory.CacheComponents<?> cacheComponents,
        DefinitionCallback definitionCallback,
        HashSet<String> waitingFor)
    {
        this.cacheName = cacheName;
        this.cacheComponents = cacheComponents;
        this.definitionCallback = definitionCallback;
        this.waitingFor = waitingFor;
    }

    void agentDefinitionComplete(String agentId)
    {
        waitingFor.remove(agentId);
        checkCompletion();
    }

    public void agentDefinitionFailed(String agentId, String errorMessage)
    {
        waitingFor.remove(agentId);
        errors.add(
            "Agent " + agentId + " failed to load cache definition for cache " + cacheName + " due to " +
                errorMessage);
        checkCompletion();
    }

    private void checkCompletion()
    {
        if (waitingFor.isEmpty())
        {
            if (errors.isEmpty())
            {
                //noinspection unchecked
                definitionCallback.onSuccess.accept(
                    cacheName,
                    new DistributedCacheStatus(
                        new CacheDefinition(
                            cacheName,
                            cacheComponents.cacheClass,
                            cacheComponents.eventLoader,
                            cacheComponents.millitimeExtractor,
                            cacheComponents.bucketSize,
                            cacheComponents.serializer),
                        new HashSet<>(),
                        new HashMap<>()
                    ));
            }
            else
            {
                definitionCallback.onError.accept("Failed to define cache due to " + errors);
            }
        }
    }
}
