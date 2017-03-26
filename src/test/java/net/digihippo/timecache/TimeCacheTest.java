package net.digihippo.timecache;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeCacheTest {
    private final ZonedDateTime time =
            ZonedDateTime.of(2016, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    private final TimeCache timeCache = new TimeCache();

    @Test
    @Ignore
    public void rejectIterationOfAbsentCache()
    {
        // FIXME: assert, perhaps?
        timeCache.iterate(
                "nonexistent",
                time,
                time.plusDays(1),
                new ReductionDefinition<>(
                    Object::new,
                    (String s, Object o) -> {},
                    (o1, o2) -> {}),
                new IterationListener<>((o) -> Assert.fail(o.toString()), Assert::fail));
    }

}