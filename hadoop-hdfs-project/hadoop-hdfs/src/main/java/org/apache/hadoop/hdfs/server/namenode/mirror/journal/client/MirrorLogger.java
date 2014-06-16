/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.mirror.journal.client;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * Interface for a remote log to mirror active namenode.
 * 
 */
interface MirrorLogger {

  interface Factory {
    MirrorLogger createLogger(Configuration conf,
        String journalId, URI addr, String regionId);
  }

  /**
   * Send a batch of edits to the logger.
   * @param segmentTxId the first txid in the current segment
   * @param firstTxnId the first txid of the edits.
   * @param numTxns the number of transactions in the batch
   * @param data the actual data to be sent
   */
  public void sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data) throws IOException;

  /**
   * Begin writing a new log segment.
   * 
   * @param txid the first txid to be written to the new log
   * @param layoutVerison layout version
   */
  public void startLogSegment(long txid, int layoutVersion) throws IOException;

  /**
   * Finalize a log segment.
   * 
   * @param startTxId the first txid that was written to the segment
   * @param endTxId the last txid that was written to the segment
   */
  public void finalizeLogSegment(
      long startTxId, long endTxId) throws IOException;

  /**
   * @return the state of the last epoch on the target node.
   */
  public GetJournalStateResponseProto getJournalState(boolean resetIPCSerial,
      NamespaceInfo nsInfo) throws IOException;

  /**
   * Tear down any resources, connections, etc. The proxy may not be used
   * after this point, and any in-flight RPCs may throw an exception.
   */
  public void close();

  /**
   * Append an HTML-formatted report for this logger's status to the provided
   * StringBuilder. This is displayed on the NN web UI.
   */
  public void appendReport(StringBuilder sb);
}
