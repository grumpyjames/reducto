package net.digihippo.timecache.api;

public interface ReadBuffer
{
    boolean readBoolean();
    byte readByte();
    byte[] readBytes(int length);
    double readDouble();
    float readFloat();
    int readInt();
    long readLong();
    String readString();

    long size();
    boolean hasBytes();
}
