package io.atomix.storage.journal;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.storage.protocol.EntryType;
import java.nio.ByteBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class EntrySerializerTest {

  private final EntrySerializer serializer = new EntrySerializer();
  private final UnsafeBuffer raftLogEntryMemory = new UnsafeBuffer(ByteBuffer.allocate(1024));
  private final UnsafeBuffer zbEntryMemory = new UnsafeBuffer(ByteBuffer.allocate(1024));

  @Test
  public void shouldSerializeEntry() {
    final long term = 123;
    final long timestamp = 234;
    final long lowestPosition = 345;
    final long highestPosition = 456;
    final ByteBuffer record = ByteBuffer.wrap("cenas".getBytes());

    // serialize ZeebeEntry
    final ZeebeEntry zbEntry =
        new ZeebeEntry(term, timestamp, lowestPosition, highestPosition, record);
    final int zbEntryLength = serializer.serializeZeebeEntry(zbEntryMemory, 0, zbEntry);

    // serialize RaftLogEntry
    final RaftLogEntry entry =
        new RaftLogEntry(term, timestamp, EntryType.ZEEBE, new UnsafeBuffer(record));
    serializer.serializeRaftLogEntry(raftLogEntryMemory, 0, entry);

    // deserialize RaftLogEntry
    final RaftLogEntry deserializedEntry =
        serializer.deserializeRaftLogEntry(raftLogEntryMemory, 0);

    final ZeebeEntry deserializedZeebeEntry =
        serializer.deserializeZeebeEntry(deserializedEntry, 0);

    assertThat(zbEntry.highestPosition()).isEqualTo(deserializedZeebeEntry.highestPosition());
    assertThat(zbEntry.lowestPosition()).isEqualTo(deserializedZeebeEntry.lowestPosition());
    assertThat(zbEntry.data()).isEqualTo(deserializedZeebeEntry.data());
  }
}
