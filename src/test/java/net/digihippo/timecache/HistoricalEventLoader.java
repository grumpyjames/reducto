package net.digihippo.timecache;

import net.digihippo.timecache.api.EventLoader;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

class HistoricalEventLoader implements EventLoader<NamedEvent> {
    private final List<NamedEvent> events;

    HistoricalEventLoader(List<NamedEvent> events) {
        this.events = events;
    }

    @Override
    public void loadEvents(Instant fromInclusive, Instant toExclusive, Consumer<NamedEvent> sink) {
        events
            .stream()
            .filter(e -> !fromInclusive.isAfter(e.time) && e.time.isBefore(toExclusive))
            .forEach(sink);
    }
}
