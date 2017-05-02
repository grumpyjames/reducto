package net.digihippo.timecache;

import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer<T>
{
    void encode(T t, OutputStream os);
    void decode(InputStream is, T t);
}
