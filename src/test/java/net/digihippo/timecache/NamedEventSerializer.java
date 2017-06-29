package net.digihippo.timecache;

import net.digihippo.timecache.api.ReadBuffer;
import net.digihippo.timecache.api.Serializer;
import net.digihippo.timecache.api.WriteBuffer;

import java.time.Instant;

public class NamedEventSerializer implements Serializer<NamedEvent>
{
    @Override
    public void encode(NamedEvent namedEvent, WriteBuffer bb)
    {
        bb.putLong(namedEvent.time.toEpochMilli());
        bb.putString(namedEvent.name);
    }

    @Override
    public NamedEvent decode(ReadBuffer bb)
    {
        final long epochMillis = bb.readLong();
        final String name = bb.readString();
        return new NamedEvent(Instant.ofEpochMilli(epochMillis), name);
    }
}
