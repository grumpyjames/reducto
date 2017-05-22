package net.digihippo.timecache;

import net.digihippo.timecache.api.CacheComponentsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static net.digihippo.timecache.NamedEvent.event;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class ReloadAndPlaybackTest {
    private static final ZonedDateTime BEGINNING_OF_TIME =
            ZonedDateTime.of(2016, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    private static final List<NamedEvent> ALL_EVENTS = createEvents(BEGINNING_OF_TIME);

    private final TimeCache timeCache = new TimeCache(TimeCacheEvents.NO_OP);

    @SuppressWarnings("WeakerAccess") // loaded reflectively
    public static final class MinuteCacheFactory implements CacheComponentsFactory<NamedEvent>
    {
        @Override
        public CacheComponents<NamedEvent> createCacheComponents() {
            return new CacheComponents<>(
                NamedEvent.class,
                new NamedEventSerializer(),
                new HistoricalEventLoader(ALL_EVENTS),
                (NamedEvent ne) -> ne.time.toEpochMilli(),
                TimeUnit.MINUTES);
        }
    }

    @Before
    public void load()
    {
        timeCache.addAgent("agentOne", new InMemoryTimeCacheAgent("agentOne", timeCache));
        timeCache.addAgent("agentTwo", new InMemoryTimeCacheAgent("agentTwo", timeCache));

        timeCache.defineCache(
            "historicalEvents",
            MinuteCacheFactory.class.getName(),
            new DefinitionListener((e) -> { throw new RuntimeException(e); }, () -> {}));

        timeCache.installDefinitions(
            PlaybackDefinitions.class.getName(),
            new InstallationListener(() -> {}, m -> Assert.fail("found installation errors: " + m.toString())));
    }

    @Test
    public void playbackEverything()
    {
        load(BEGINNING_OF_TIME, BEGINNING_OF_TIME.plusHours(1));

        assertPlaybackContainsCorrectEvents(
            BEGINNING_OF_TIME,
                BEGINNING_OF_TIME.plusHours(1), Optional.empty());
    }

    @Test
    public void playbackEverythingExceptBananas()
    {
        load(BEGINNING_OF_TIME, BEGINNING_OF_TIME.plusHours(1));

        assertPlaybackContainsCorrectEvents(
            BEGINNING_OF_TIME,
            BEGINNING_OF_TIME.plusHours(1),
            Optional.of("banana"));
    }

    @Test
    public void requestedTimeRangeContainingOneResult()
    {
        load(BEGINNING_OF_TIME, BEGINNING_OF_TIME.plusHours(1));

        assertPlaybackContainsCorrectEvents(
                BEGINNING_OF_TIME.plusMinutes(42),
                BEGINNING_OF_TIME.plusMinutes(43), Optional.empty());
    }

    @Test
    public void requestedTimeRangeWithinSingleBucket()
    {
        load(BEGINNING_OF_TIME, BEGINNING_OF_TIME.plusHours(1));

        assertPlaybackContainsCorrectEvents(
                BEGINNING_OF_TIME.plusMinutes(43),
                BEGINNING_OF_TIME.plusMinutes(43).plusSeconds(15), Optional.empty());
    }

    @Test
    public void loadBoundariesThatAreNotBucketBoundaries()
    {
        load(BEGINNING_OF_TIME.plusSeconds(1), BEGINNING_OF_TIME.plusSeconds(10));

        assertPlaybackContainsCorrectEvents(
            BEGINNING_OF_TIME,
                BEGINNING_OF_TIME.plusSeconds(8), Optional.empty());
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
        ZonedDateTime to,
        Optional<String> filterArgs) {
        ArrayList<NamedEvent> result = new ArrayList<>();
        Optional<ByteBuffer> maybeBuffer = filterArgs.map(f -> {
            byte[] bytes = f.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 4);
            buffer.putInt(bytes.length);
            buffer.put(bytes);
            buffer.flip();

            return buffer;
        });
        timeCache.<List<NamedEvent>>iterate(
                "historicalEvents",
                from,
                to,
                PlaybackDefinitions.class.getName(),
                "default",
                maybeBuffer,
                new IterationListener(
                    (bb) -> result.addAll(new ListSerializer<>(new NamedEventSerializer()).decode(bb)),
                    Assert::fail
                ));

        final Predicate<NamedEvent> pred =
            filterArgs
                .map(f -> (Predicate<NamedEvent>) namedEvent -> !namedEvent.name.contains(f))
                .orElse(namedEvent -> true);
        assertThat(result, containsInAnyOrder(ALL_EVENTS
                .stream()
                .filter(e -> !from.toInstant().isAfter(e.time) && e.time.isBefore(to.toInstant()))
                .filter(pred)
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
