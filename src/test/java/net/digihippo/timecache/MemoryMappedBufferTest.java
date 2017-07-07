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

        rw.prepareForRead();
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

        @SuppressWarnings("SimplifiableIfStatement")
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

}
