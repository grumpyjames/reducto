package net.digihippo.timecache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class LoadDefinitionsTest {

    private final TimeCache timeCache = new TimeCache();

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
        final Map<String, String> errors = new HashMap<>();
        timeCache.installDefinitions(
            PlaybackDefinitions.class.getName(),
            new InstallationListener(
                () -> Assert.fail("should fail to install"), errors::putAll));

        timeCache.installationComplete("agentOne", PlaybackDefinitions.class.getName());
        timeCache.installationError("agentTwo", PlaybackDefinitions.class.getName(), "oops");
        timeCache.installationError("agentThree", PlaybackDefinitions.class.getName(), "moo");

        assertThat(errors,
            allOf(hasEntry("agentTwo", "oops"), hasEntry("agentThree", "moo")));
    }

    @Test
    public void failureIsOnlyReportedOnceEveryAgentHasCheckedIn()
    {
        final Map<String, String> errors = new HashMap<>();
        timeCache.installDefinitions(
            PlaybackDefinitions.class.getName(),
            new InstallationListener(
                () -> Assert.fail("should fail to install"), errors::putAll));

        timeCache.installationComplete("agentOne", PlaybackDefinitions.class.getName());
        timeCache.installationError("agentTwo", PlaybackDefinitions.class.getName(), "oops");

        assertThat(errors.entrySet(), is(empty()));

        timeCache.installationComplete("agentThree", PlaybackDefinitions.class.getName());
    }

    @Test
    public void successIsOnlyReportedOnceEveryAgentHasCheckedIn()
    {
        final List<String> success = new ArrayList<>();
        timeCache.installDefinitions(
            PlaybackDefinitions.class.getName(),
            new InstallationListener(
                () -> success.add("woot"), (m -> Assert.fail(m.toString()))));

        timeCache.installationComplete("agentOne", PlaybackDefinitions.class.getName());
        timeCache.installationComplete("agentTwo", PlaybackDefinitions.class.getName());

        assertThat(success, is(empty()));

        timeCache.installationComplete("agentThree", PlaybackDefinitions.class.getName());

        assertThat(success, contains("woot"));
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
            String definitionName) {

        }

        @Override
        public void defineCache(String cacheName, String cacheComponentFactoryClass) {

        }
    }
}
