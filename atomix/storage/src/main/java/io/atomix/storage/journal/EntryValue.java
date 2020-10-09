package io.atomix.storage.journal;

import io.atomix.storage.protocol.EntryType;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public interface EntryValue {
  long term();
  long timestamp();
  EntryType type();
  int serialize(EntrySerializer serializer, MutableDirectBuffer dest, int offset);
}
