package net.digihippo.timecache;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class GoodErrorMessagesTest
{
    private final TimeCacheActions timeCache = new TimeCache(TimeCacheEvents.NO_OP);

    @Test
    public void missingCacheFactoryClassOnServer()
    {
        final List<String> errors = new ArrayList<>();
        timeCache.defineCache(
            "moose",
            "com.example.NonExistent",
            new DefinitionListener(Assert::fail, errors::add));

        assertThat(errors,
            containsInAnyOrder("Unable to define cache 'moose'," +
                " encountered java.lang.ClassNotFoundException: com.example.NonExistent"));
    }

}
