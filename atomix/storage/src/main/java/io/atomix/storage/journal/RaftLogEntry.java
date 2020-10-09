/*
 * Copyright 2015-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.storage.journal;

import static com.google.common.base.MoreObjects.toStringHelper;

import io.atomix.storage.protocol.EntryType;
import org.agrona.DirectBuffer;

/** Stores a state change in a RaftLog. */
public class RaftLogEntry {

  private long term;
  private long timestamp;
  private EntryType entryType;
  private DirectBuffer entry;

  public RaftLogEntry() {}

  public RaftLogEntry(
      final long term, final long timestamp, final EntryType entryType, final DirectBuffer entry) {
    this.term = term;
    this.timestamp = timestamp;
    this.entryType = entryType;
    this.entry = entry;
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long term() {
    return term;
  }

  public void setTerm(final long term) {
    this.term = term;
  }

  public void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }

  public void setEntryType(final EntryType entryType) {
    this.entryType = entryType;
  }

  public void setEntry(final DirectBuffer entry) {
    this.entry = entry;
  }

  public long timestamp() {
    return timestamp;
  }

  public EntryType type() {
    return entryType;
  }

  public DirectBuffer entry() {
    return entry;
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("term", term).toString();
  }
}
