package net.digihippo.timecache.api;

import java.util.Map;

public interface DefinitionSource
{
    Map<String, ReductionDefinition<?,?>> definitions();
}
