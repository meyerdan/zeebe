/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.storage.journal;

import io.atomix.storage.protocol.EntryDecoder;
import io.atomix.storage.protocol.EntryEncoder;
import io.atomix.storage.protocol.MessageHeaderDecoder;
import io.atomix.storage.protocol.MessageHeaderEncoder;
import io.atomix.storage.protocol.ZeebeDecoder;
import io.atomix.storage.protocol.ZeebeEncoder;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class EntrySerializer {

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final EntryEncoder rfEncoder = new EntryEncoder();
  private final EntryDecoder rfDecoder = new EntryDecoder();
  private final ZeebeEncoder zbEncoder = new ZeebeEncoder();
  private final ZeebeDecoder zbDecoder = new ZeebeDecoder();

  public void serializeRaftLogEntry(
      final MutableDirectBuffer buffer, final int offset, final RaftLogEntry entry) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(rfEncoder.sbeBlockLength())
        .templateId(rfEncoder.sbeTemplateId())
        .schemaId(rfEncoder.sbeSchemaId())
        .version(rfEncoder.sbeSchemaVersion());

    rfEncoder.wrap(buffer, offset + headerEncoder.encodedLength());
    rfEncoder
        .term(entry.term())
        .timestamp(entry.timestamp())
        .entryType(entry.type())
        .putEntry(entry.entry(), 0, entry.entry().capacity());
  }

  public RaftLogEntry deserializeRaftLogEntry(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    rfDecoder.wrap(
        buffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final UnsafeBuffer entryBuffer = new UnsafeBuffer(ByteBuffer.allocate(rfDecoder.entryLength()));
    rfDecoder.getEntry(entryBuffer, 0, rfDecoder.entryLength());

    return new RaftLogEntry(
        rfDecoder.term(), rfDecoder.timestamp(), rfDecoder.entryType(), entryBuffer);
  }

  public int serializeZeebeEntry(
      final MutableDirectBuffer buffer, final int offset, final ZeebeEntry entry) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(zbEncoder.sbeBlockLength())
        .templateId(zbEncoder.sbeTemplateId())
        .schemaId(zbEncoder.sbeSchemaId())
        .version(zbEncoder.sbeSchemaVersion());

    zbEncoder.wrap(buffer, offset + headerEncoder.encodedLength());
    zbEncoder
        .lowestPosition(entry.lowestPosition())
        .highestPosition(entry.highestPosition())
        .putData(entry.data(), 0, entry.data().capacity());

    return zbEncoder.encodedLength();
  }

  public ZeebeEntry deserializeZeebeEntry(final RaftLogEntry entry, final int offset) {
    headerDecoder.wrap(entry.entry(), offset);
    zbDecoder.wrap(
        entry.entry(),
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final UnsafeBuffer dataBuffer = new UnsafeBuffer(ByteBuffer.allocate(zbDecoder.dataLength()));
    zbDecoder.getData(dataBuffer, 0, zbDecoder.dataLength());

    return new ZeebeEntry(
        entry.term(),
        entry.timestamp(),
        zbDecoder.lowestPosition(),
        zbDecoder.highestPosition(),
        dataBuffer);
  }
}
