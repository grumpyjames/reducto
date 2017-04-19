package net.digihippo.timecache;

import java.time.Instant;
import java.util.function.Consumer;

public interface EventLoader<T> {
    void loadEvents(
        final Instant fromInclusive,
        final Instant toExclusive,
        final Consumer<T> sink);
}
