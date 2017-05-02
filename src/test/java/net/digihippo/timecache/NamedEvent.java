package net.digihippo.timecache;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZonedDateTime;

public final class NamedEvent {
    final Instant time;
    final String name;

    public static class Broken implements Serializer<NamedEvent>
    {
        @Override
        public void encode(NamedEvent namedEvent, OutputStream os)
        {

        }

        @Override
        public void decode(InputStream is, NamedEvent namedEvent)
        {

        }
    }

    public static NamedEvent event(final long epochMillis, final String name) {
        return new NamedEvent(Instant.ofEpochMilli(epochMillis), name);
    }

    public static NamedEvent event(final ZonedDateTime dateTime, final String name) {
        return new NamedEvent(dateTime.toInstant(), name);
    }

    public NamedEvent(Instant time, String name) {
        this.time = time;
        this.name = name;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NamedEvent that = (NamedEvent) o;

        if (!time.equals(that.time)) return false;
        return name.equals(that.name);

    }

    @Override
    public int hashCode() {
        int result = time.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "NamedEvent{" +
                "time=" + time +
                ", name='" + name + '\'' +
                '}';
    }
}
