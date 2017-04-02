package net.digihippo.timecache;

public interface DefinitionSink
{
    void addReductionDefinition(
        String name,
        ReductionDefinition<?, ?> reductionDefinition
    );
}
