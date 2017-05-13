package net.digihippo.timecache;

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
    public void encode(List<T> namedEvents, ByteBuffer bb)
    {
        bb.putInt(namedEvents.size());
        for (T t : namedEvents)
        {
            inner.encode(t, bb);
        }
    }

    @Override
    public List<T> decode(ByteBuffer bb)
    {
        int size = bb.getInt();
        final List<T> events = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            events.add(inner.decode(bb));
        }
        return events;
    }
}
