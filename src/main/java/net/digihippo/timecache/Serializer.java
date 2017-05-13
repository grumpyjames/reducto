package net.digihippo.timecache;

import java.nio.ByteBuffer;

public interface Serializer<T>
{
    void encode(T t, ByteBuffer bb);
    T decode(ByteBuffer bb);
}
