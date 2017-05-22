package net.digihippo.timecache;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Optional;

public interface TimeCacheAdministration
{
    void installDefinitions(String name, InstallationListener installationListener);

    <U> void iterate(
        String cacheName,
        ZonedDateTime from,
        ZonedDateTime toExclusive,
        String definingClass,
        String iterateeName,
        Optional<ByteBuffer> filterArguments,
        IterationListener iterationListener);

    void defineCache(
        String cacheName,
        String cacheComponentFactoryClass,
        DefinitionListener definitionListener);
}
