package net.digihippo.timecache;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class TimeCacheTest {
    private final ZonedDateTime time =
            ZonedDateTime.of(2016, 11, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    private final TimeCache timeCache = new TimeCache();

    @Test
    public void rejectIterationOfAbsentCache()
    {
        final List<String> errors = new ArrayList<>();
        timeCache.iterate(
                "nonexistent",
                time,
                time.plusDays(1),
                new ReductionDefinition<>(
                    Object::new,
                    (String s, Object o) -> {},
                    (o1, o2) -> {}),
                new IterationListener<>(
                        (o) -> Assert.fail(o.toString()), errors::add));

        assertThat(errors, contains("Cache 'nonexistent' not found"));
    }

}