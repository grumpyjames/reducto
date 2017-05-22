package net.digihippo.timecache;

import net.digihippo.timecache.api.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

@SuppressWarnings("WeakerAccess") // loaded reflectively
public class PlaybackDefinitions implements DefinitionSource
{
    @Override
    public Map<String, ReductionDefinition<?, ?, ?>> definitions() {
        HashMap<String, ReductionDefinition<?, ?, ?>> result = new HashMap<>();

        result.put(
            "default",
            new ReductionDefinition<NamedEvent, List<NamedEvent>, String>(
                ArrayList::new,
                List::add,
                List::addAll,
                new ListSerializer<>(new NamedEventSerializer()),
                new FilterDefinition<>(
                    new Serializer<String>()
                    {
                        @Override
                        public void encode(String s, WriteBuffer bb)
                        {
                            final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                            bb.putInt(bytes.length);
                            bb.putBytes(bytes);
                        }

                        @Override
                        public String decode(ByteBuffer bb)
                        {
                            final int length = bb.getInt();
                            final byte[] body = new byte[length];
                            bb.get(body, 0, length);
                            return new String(body, StandardCharsets.UTF_8);
                        }
                    },
                    s ->
                        s.map(str ->
                            (Predicate<NamedEvent>) namedEvent -> !namedEvent.name.contains(str))
                                .orElse((NamedEvent ne) -> true)
                )));

        return result;
    }
}
