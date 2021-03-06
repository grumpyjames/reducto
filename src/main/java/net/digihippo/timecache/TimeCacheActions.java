package net.digihippo.timecache;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Optional;

public interface TimeCacheActions
{
    void installDefinitions(
        String name,
        InstallationListener installationListener);

    void iterate(
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

    void load(
        String cacheName,
        ZonedDateTime fromInclusive,
        ZonedDateTime toExclusive,
        LoadListener loadListener);
}
