package net.digihippo.timecache;

import net.digihippo.timecache.api.ReadBuffer;
import net.digihippo.timecache.api.Serializer;
import net.digihippo.timecache.api.WriteBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class MemoryMappedBufferTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void roundTripTest() throws IOException
    {
        final File file = folder.newFile("bucket");

        MemoryMappedBuffer rw = new MemoryMappedBuffer(new RandomAccessFile(file, "rw"));

        Thingy t = new Thingy("boo", 235323, "nfjnsdf sdfsdfs");

        ThingySerializer sz = new ThingySerializer();
        sz.encode(t, rw);

        rw.flip();
        Thingy roundTripped = sz.decode(rw);

        assertEquals(roundTripped, t);
    }

    private static class ThingySerializer implements Serializer<Thingy>
    {
        @Override
        public void encode(Thingy thingy, WriteBuffer wb)
        {
            wb.putString(thingy.one);
            wb.putInt(thingy.two);
            wb.putString(thingy.three);
        }

        @Override
        public Thingy decode(ReadBuffer bb)
        {
            String one = bb.readString();
            int two = bb.readInt();
            String three = bb.readString();
            return new Thingy(one, two, three);
        }
    }

    private static class Thingy
    {
        private final String one;
        private final int two;
        private final String three;

        private Thingy(String one, int two, String three)
        {
            this.one = one;
            this.two = two;
            this.three = three;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Thingy thingy = (Thingy) o;

            if (two != thingy.two) return false;
            if (one != null ? !one.equals(thingy.one) : thingy.one != null) return false;
            return !(three != null ? !three.equals(thingy.three) : thingy.three != null);

        }

        @Override
        public int hashCode()
        {
            int result = one != null ? one.hashCode() : 0;
            result = 31 * result + two;
            result = 31 * result + (three != null ? three.hashCode() : 0);
            return result;
        }

        @Override
        public String toString()
        {
            return "Thingy{" +
                "one='" + one + '\'' +
                ", two=" + two +
                ", three='" + three + '\'' +
                '}';
        }
    }

    private static class MemoryMappedBuffer implements ReadBuffer, WriteBuffer
    {
        private final RandomAccessFile randomAccessFile;

        private MemoryMappedBuffer(RandomAccessFile randomAccessFile)
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
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void flip()
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

}
