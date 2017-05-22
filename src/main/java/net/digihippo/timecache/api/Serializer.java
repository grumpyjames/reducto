package net.digihippo.timecache.api;

import java.nio.ByteBuffer;

public interface Serializer<T>
{
    void encode(T t, WriteBuffer wb);
    T decode(ByteBuffer bb);
}
