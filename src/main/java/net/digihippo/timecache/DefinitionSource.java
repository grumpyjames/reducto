package net.digihippo.timecache;

import java.util.Map;

public interface DefinitionSource
{
    Map<String, ReductionDefinition<?,?>> definitions();
}
