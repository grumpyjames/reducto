package net.digihippo.timecache;

import net.digihippo.timecache.api.ReadBuffer;
import net.digihippo.timecache.api.Serializer;
import net.digihippo.timecache.api.WriteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class ListSerializer<T> implements Serializer<List<T>>
{
    private Serializer<T> inner;

    ListSerializer(Serializer<T> inner)
    {
        this.inner = inner;
    }

    @Override
    public void encode(List<T> namedEvents, WriteBuffer bb)
    {
        bb.putInt(namedEvents.size());
        for (T t : namedEvents)
        {
            inner.encode(t, bb);
        }
    }

    @Override
    public List<T> decode(ReadBuffer bb)
    {
        int size = bb.readInt();
        final List<T> events = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            events.add(inner.decode(bb));
        }

        return events;
    }
}
