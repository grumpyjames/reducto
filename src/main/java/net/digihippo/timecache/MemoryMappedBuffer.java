package net.digihippo.timecache;

import net.digihippo.timecache.api.ReadBuffer;
import net.digihippo.timecache.api.WriteBuffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

class MemoryMappedBuffer implements ReadBuffer, WriteBuffer
{
    private final RandomAccessFile randomAccessFile;

    MemoryMappedBuffer(RandomAccessFile randomAccessFile)
    {
        this.randomAccessFile = randomAccessFile;
    }

    @Override
    public void putBoolean(boolean b)
    {
        try
        {
            randomAccessFile.writeBoolean(b);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putByte(byte b)
    {
        try
        {
            randomAccessFile.writeByte(b);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putBytes(byte[] bytes)
    {
        try
        {
            randomAccessFile.write(bytes);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putBytes(byte[] bytes, int offset, int length)
    {
        try
        {
            randomAccessFile.write(bytes, offset, length);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putDouble(double d)
    {
        try
        {
            randomAccessFile.writeDouble(d);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putFloat(float f)
    {
        try
        {
            randomAccessFile.writeFloat(f);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putInt(int i)
    {
        try
        {
            randomAccessFile.writeInt(i);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putLong(long l)
    {
        try
        {
            randomAccessFile.writeLong(l);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putString(String s)
    {
        try
        {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            randomAccessFile.writeInt(bytes.length);
            randomAccessFile.write(bytes);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean readBoolean()
    {
        try
        {
            return randomAccessFile.readBoolean();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte readByte()
    {
        try
        {
            return randomAccessFile.readByte();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] readBytes(int length)
    {
        byte[] result = new byte[length];
        try
        {
            randomAccessFile.read(result);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public double readDouble()
    {
        try
        {
            return randomAccessFile.readDouble();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public float readFloat()
    {
        try
        {
            return randomAccessFile.readFloat();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt()
    {
        try
        {
            return randomAccessFile.readInt();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong()
    {
        try
        {
            return randomAccessFile.readLong();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String readString()
    {
        try
        {
            final int length = randomAccessFile.readInt();
            byte[] bytes = readBytes(length);

            return new String(bytes, StandardCharsets.UTF_8);

        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long size()
    {
        try
        {
            return randomAccessFile.length();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasBytes()
    {
        try
        {
            return randomAccessFile.getFilePointer() < randomAccessFile.length();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void prepareForRead()
    {
        try
        {
            randomAccessFile.seek(0L);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
