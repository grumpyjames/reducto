package net.digihippo.timecache;

import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class TimeCacheTest {
    private final ZonedDateTime beginningOfTime =
            ZonedDateTime.of(2016, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC"));


    @Test
    public void loadAndPlayback()
    {
        TimeCache timeCache = new TimeCache();

        timeCache.addAgent(new TimeCache.TimeCacheAgent());
        timeCache.addAgent(new TimeCache.TimeCacheAgent());

        List<NamedEvent> allEvents = createEvents(beginningOfTime);
        timeCache.defineCache(
                "historicalEvents",
                NamedEvent.class,
                new HistoricalEventLoader(allEvents));

        timeCache.load(
                "historicalEvents",
                beginningOfTime,
                beginningOfTime.plusHours(1),
                TimeUnit.MINUTES);

        ArrayList<NamedEvent> result = new ArrayList<>();
        timeCache.iterate(
                "historicalEvents",
                beginningOfTime,
                beginningOfTime.plusHours(1),
                NamedEvent.class,
                result,
                (NamedEvent namedEvent, List<NamedEvent> namedEvents) -> namedEvents.add(namedEvent),
                List::addAll);

        assertThat(result, containsInAnyOrder(allEvents.toArray()));
    }

    private List<NamedEvent> createEvents(final ZonedDateTime minimumTime)
    {
        long baseTime = minimumTime.toInstant().toEpochMilli();
        return Arrays.asList(
                tse(baseTime + 1000L, "foo"),
                tse(baseTime + TimeUnit.MINUTES.toMillis(42L), "baz"),
                tse(baseTime + TimeUnit.MINUTES.toMillis(43L) + 1433L, "banana"),
                tse(baseTime + TimeUnit.MINUTES.toMillis(54L), "bananaAgain"));
    }

    private class HistoricalEventLoader implements EventLoader<NamedEvent> {
        private final List<NamedEvent> events;

        public HistoricalEventLoader(List<NamedEvent> events) {
            this.events = events;
        }

        @Override
        public void loadEvents(Instant fromInclusive, Instant toExclusive, Consumer<NamedEvent> sink) {
            events.stream().filter(
                    e -> !fromInclusive.isAfter(e.time) && e.time.isBefore(toExclusive)
            ).forEach(sink);
        }
    }

    public static NamedEvent tse(final long epochMillis, final String name) {
        return new NamedEvent(Instant.ofEpochMilli(epochMillis), name);
    }

    public static final class NamedEvent {
        private final Instant time;
        private final String name;

        public NamedEvent(Instant time, String name) {
            this.time = time;
            this.name = name;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NamedEvent that = (NamedEvent) o;

            if (!time.equals(that.time)) return false;
            return name.equals(that.name);

        }

        @Override
        public int hashCode() {
            int result = time.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "NamedEvent{" +
                    "time=" + time +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
