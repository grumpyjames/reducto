package net.digihippo.timecache;

import net.digihippo.timecache.api.CacheComponentsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class LoadDefinitionsTest {

    private final TimeCache timeCache = new TimeCache(TimeCacheEvents.NO_OP);

    @Before
    public void setup()
    {
        timeCache.addAgent("agentOne", new NoOpAgent());
        timeCache.addAgent("agentTwo", new NoOpAgent());
        timeCache.addAgent("agentThree", new NoOpAgent());
    }

    @Test
    public void failureToLoadOnASingleAgentIsALoadFailure()
    {
        final List<String> errors = new ArrayList<>();
        timeCache.installDefinitions(
            PlaybackDefinitions.class.getName(),
            new InstallationListener(
                () -> Assert.fail("should fail to install"), errors::add));

        timeCache.installationComplete("agentOne", PlaybackDefinitions.class.getName());
        timeCache.installationError("agentTwo", PlaybackDefinitions.class.getName(), "oops");
        timeCache.installationError("agentThree", PlaybackDefinitions.class.getName(), "moo");

        assertThat(errors,
            hasItem(allOf(containsString("agentTwo: oops"), containsString("agentThree: moo"))));
    }

    @Test
    public void failureIsOnlyReportedOnceEveryAgentHasCheckedIn()
    {
        final List<String> errors = new ArrayList<>();
        timeCache.installDefinitions(
            PlaybackDefinitions.class.getName(),
            new InstallationListener(
                () -> Assert.fail("should fail to install"), errors::add));

        timeCache.installationComplete("agentOne", PlaybackDefinitions.class.getName());
        timeCache.installationError("agentTwo", PlaybackDefinitions.class.getName(), "oops");

        assertThat(errors, is(empty()));

        timeCache.installationComplete("agentThree", PlaybackDefinitions.class.getName());
    }

    @Test
    public void successIsOnlyReportedOnceEveryAgentHasCheckedIn()
    {
        final List<String> success = new ArrayList<>();
        timeCache.installDefinitions(
            PlaybackDefinitions.class.getName(),
            new InstallationListener(
                () -> success.add("woot"), (Assert::fail)));

        timeCache.installationComplete("agentOne", PlaybackDefinitions.class.getName());
        timeCache.installationComplete("agentTwo", PlaybackDefinitions.class.getName());

        assertThat(success, is(empty()));

        timeCache.installationComplete("agentThree", PlaybackDefinitions.class.getName());

        assertThat(success, contains("woot"));
    }

    @Test
    public void reportDefinitionFailures()
    {
        final List<String> fails = new ArrayList<>();
        timeCache.defineCache(
            "moose",
            MinuteCacheFactory.class.getName(),
            new DefinitionListener(
                () -> Assert.fail("should not succeed"), fails::add));

        timeCache.cacheDefined("agentOne", "moose");
        timeCache.cacheDefinitionFailed("agentTwo", "moose", "merde");
        timeCache.cacheDefined("agentThree", "moose");

        assertThat(
            fails,
            equalTo(
                singletonList(
                    "Failed to define cache due to " +
                        "[Agent agentTwo failed to load cache definition" +
                        " for cache moose due to merde]")));
    }

    @Test
    public void reportLoadFailure()
    {
        final List<String> fails = new ArrayList<>();
        timeCache.defineCache(
            "moose",
            MinuteCacheFactory.class.getName(),
            new DefinitionListener(
                () -> {}, Assert::fail));

        timeCache.cacheDefined("agentOne", "moose");
        timeCache.cacheDefined("agentTwo", "moose");
        timeCache.cacheDefined("agentThree", "moose");

        ZonedDateTime start = ZonedDateTime.ofInstant(Instant.ofEpochMilli(60000), ZoneId.of("UTC"));
        timeCache.load(
            "moose",
            start,
            start.plusMinutes(3),
            new LoadListener(
                () -> Assert.fail("should fail"),
                fails::add));

        timeCache.loadComplete("agentOne", "moose", start.toInstant().toEpochMilli(), -1L);
        timeCache.loadComplete("agentTwo", "moose", start.plusMinutes(1).toInstant().toEpochMilli(), -1L);
        timeCache.loadFailure(
            "agentThree",
            "moose",
            start.plusMinutes(2).toInstant().toEpochMilli(),
            "a bad thing happened");

        assertThat(
            fails,
            equalTo(
                singletonList(
                    "Encountered errors during load " +
                        "[Agent agentThree failed to load bucket 180000 due to a bad thing happened]")));

    }


    private static final class NoOpAgent implements TimeCacheAgent {
        @Override
        public void installDefinitions(String className) {

        }

        @Override
        public void populateBucket(
            String cacheName,
            long currentBucketStart,
            long currentBucketEnd) {

        }

        @Override
        public void iterate(
            String cacheName,
            long iterationKey,
            ZonedDateTime from,
            ZonedDateTime toExclusive,
            String installingClass,
            String definitionName,
            Optional<ByteBuffer> wireFilterArgs) {

        }

        @Override
        public void defineCache(String cacheName, String cacheComponentFactoryClass) {

        }
    }

    @SuppressWarnings("WeakerAccess") // loaded reflectively
    public static final class MinuteCacheFactory implements CacheComponentsFactory<NamedEvent>
    {
        @Override
        public CacheComponents<NamedEvent> createCacheComponents() {
            return new CacheComponents<>(
                NamedEvent.class,
                new NamedEventSerializer(),
                new HistoricalEventLoader(Collections.emptyList()),
                (NamedEvent ne) -> ne.time.toEpochMilli(),
                TimeUnit.MINUTES);
        }
    }

}
