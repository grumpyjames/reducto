package net.digihippo.timecache.api;

public interface Serializer<T>
{
    void encode(T t, WriteBuffer wb);
    T decode(ReadBuffer bb);
}
