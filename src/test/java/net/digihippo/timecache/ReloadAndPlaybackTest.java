package net.digihippo.timecache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static net.digihippo.timecache.NamedEvent.event;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class ReloadAndPlaybackTest {
    private final ZonedDateTime beginningOfTime =
            ZonedDateTime.of(2016, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    private final TimeCache timeCache = new TimeCache();
    private final List<NamedEvent> allEvents = createEvents(beginningOfTime);

    @Before
    public void load()
    {
        timeCache.addAgent(new InMemoryTimeCacheAgent("agentOne", timeCache));
        timeCache.addAgent(new InMemoryTimeCacheAgent("agentTwo", timeCache));

        timeCache.defineCache(
                "historicalEvents",
                NamedEvent.class,
                new HistoricalEventLoader(allEvents),
                (NamedEvent ne) -> ne.time.toEpochMilli(),
                TimeUnit.MINUTES);
    }

    @Test
    public void playbackEverything()
    {
        load(beginningOfTime, beginningOfTime.plusHours(1));

        assertPlaybackContainsCorrectEvents(
                beginningOfTime,
                beginningOfTime.plusHours(1));
    }

    @Test
    public void requestedTimeRangeContainingOneResult()
    {
        load(beginningOfTime, beginningOfTime.plusHours(1));

        assertPlaybackContainsCorrectEvents(
                beginningOfTime.plusMinutes(42),
                beginningOfTime.plusMinutes(43));
    }

    @Test
    public void requestedTimeRangeWithinSingleBucket()
    {
        load(beginningOfTime, beginningOfTime.plusHours(1));

        assertPlaybackContainsCorrectEvents(
                beginningOfTime.plusMinutes(43),
                beginningOfTime.plusMinutes(43).plusSeconds(15));
    }

    @Test
    public void loadBoundariesThatAreNotBucketBoundaries()
    {
        load(beginningOfTime.plusSeconds(1), beginningOfTime.plusSeconds(10));

        assertPlaybackContainsCorrectEvents(
                beginningOfTime,
                beginningOfTime.plusSeconds(8));
    }

    private void load(ZonedDateTime from, ZonedDateTime to) {
        timeCache.load(
                "historicalEvents",
                from,
                to,
                new LoadListener(() -> {}, (e) -> {}));
    }

    private void assertPlaybackContainsCorrectEvents(
            ZonedDateTime from,
            ZonedDateTime to) {
        ArrayList<NamedEvent> result = new ArrayList<>();
        timeCache.iterate(
                "historicalEvents",
                from,
                to,
                new ReductionDefinition<>(
                    ArrayList::new,
                    (NamedEvent namedEvent, List<NamedEvent> namedEvents) -> namedEvents.add(namedEvent),
                    List::addAll),
                new IterationListener<>(
                    result::addAll,
                    Assert::fail
                ));

        assertThat(result, containsInAnyOrder(allEvents
                .stream()
                .filter(e -> !from.toInstant().isAfter(e.time) && e.time.isBefore(to.toInstant()))
                .collect(Collectors.toList())
                .toArray()));
    }

    private static List<NamedEvent> createEvents(final ZonedDateTime minimumTime)
    {
        long baseTime = minimumTime.toInstant().toEpochMilli();
        return Arrays.asList(
                event(baseTime + 1000L, "foo"),
                event(baseTime + TimeUnit.MINUTES.toMillis(42L), "baz"),
                event(baseTime + TimeUnit.MINUTES.toMillis(43L) + 1433L, "banana"),
                event(baseTime + TimeUnit.MINUTES.toMillis(43L) + TimeUnit.SECONDS.toMillis(52), "overripe banana"),
                event(baseTime + TimeUnit.MINUTES.toMillis(54L), "bananaAgain"));
    }

}
