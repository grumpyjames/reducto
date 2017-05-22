package net.digihippo.timecache;

import net.digihippo.timecache.api.Serializer;
import net.digihippo.timecache.api.WriteBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
    public NamedEvent decode(ByteBuffer bb)
    {
        final long epochMillis = bb.getLong();
        final String name = readString(bb);
        return new NamedEvent(Instant.ofEpochMilli(epochMillis), name);
    }

    private String readString(ByteBuffer bb)
    {
        int length = bb.getInt();
        final byte[] bytes = new byte[length];
        bb.get(bytes, 0, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
