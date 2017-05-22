package net.digihippo.timecache.api;

public interface WriteBuffer
{
    void putBoolean(boolean b);
    void putByte(byte b);
    void putBytes(byte[] bytes);
    void putBytes(byte[] bytes, int offset, int length);
    void putDouble(double d);
    void putFloat(float f);
    void putInt(int i);
    void putLong(long l);
    void putString(String s);
}
