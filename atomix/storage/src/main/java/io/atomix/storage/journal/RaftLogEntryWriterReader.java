package io.atomix.storage.journal;

import io.atomix.storage.protocol.EntryDecoder;
import io.atomix.storage.protocol.EntryEncoder;
import io.zeebe.protocol.impl.encoding.SbeBufferWriterReader;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;
import org.agrona.sbe.MessageEncoderFlyweight;

public class RaftLogEntryWriterReader extends SbeBufferWriterReader {

  private final EntryDecoder decoder = new EntryDecoder();
  private final EntryEncoder encoder = new EntryEncoder();
  private RaftLogEntry entry = new RaftLogEntry();

  @Override
  protected MessageEncoderFlyweight getBodyEncoder() {
    return encoder;
  }

  @Override
  protected MessageDecoderFlyweight getBodyDecoder() {
    return decoder;
  }

  @Override
  public int getLength() {
    return super.getLength() + EntryEncoder.entryHeaderLength() + entry.entry().capacity();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    super.write(buffer, offset);

    encoder
        .term(entry.term())
        .timestamp(entry.timestamp())
        .entryType(entry.type())
        .putEntry(entry.entry(), 0, entry.entry().capacity());
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    super.wrap(buffer, offset, length);

    entry.setTerm(decoder.term());
    entry.setTimestamp(decoder.timestamp());
    entry.setEntryType(decoder.entryType());
    decoder.wrapEntry(entry.entry());
    //    entry
    //        .entry()
    //        .wrap(buffer, decoder.limit() + EntryDecoder.entryHeaderLength(),
    // decoder.entryLength());
  }

  public void setEntry(final RaftLogEntry entry) {
    this.entry = entry;
  }
}
