package net.digihippo.timecache;

import net.digihippo.timecache.api.DefinitionSource;
import net.digihippo.timecache.api.ReductionDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlaybackDefinitions implements DefinitionSource
{
    @Override
    public Map<String, ReductionDefinition<?, ?>> definitions() {
        HashMap<String, ReductionDefinition<?, ?>> result = new HashMap<>();

        result.put(
            "default",
            new ReductionDefinition<NamedEvent, List<NamedEvent>>(
                ArrayList::new,
                List::add,
                List::addAll,
                new ListSerializer<NamedEvent>(new NamedEventSerializer())));

        return result;
    }
}
