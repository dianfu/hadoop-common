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
package org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol used to communicate between {@link MirrorJournalManager}
 * and active namenode of mirror cluster
 * 
 * This is responsible for sending edits as well as coordinating
 * recovery of the nodes.
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface MirrorJournalProtocol {
  public static final long versionID = 1L;

  /**
   * Get the current state of the journal, including the most recent
   * epoch number and the HTTP port.
   */
  @Idempotent
  public JournalState getJournalState(boolean resetIPCSerial,
      NamespaceInfo nsInfo) throws IOException;

  /**
   * Journal edit records.
   * This message is sent by the active name-node to the JournalNodes
   * to write edits to their local logs.
   */
  @AtMostOnce
  public void journal(RequestInfo reqInfo,
                      long segmentTxId,
                      long firstTxnId,
                      int numTxns,
                      byte[] records) throws IOException;

  /**
   * Start writing to a new log segment on the JournalNode.
   * Before calling this, one should finalize the previous segment
   * using {@link #finalizeLogSegment(RequestInfo, long, long)}.
   * 
   * @param txid the first txid in the new log
   * @param layoutVersion layout version
   */
  @AtMostOnce
  public void startLogSegment(RequestInfo reqInfo,
      long txid, int layoutVersion) throws IOException;

  /**
   * Finalize the given log segment on the JournalNode. The segment
   * is expected to be in-progress and starting at the given startTxId.
   *
   * @param startTxId the starting transaction ID of the log
   * @param endTxId the expected last transaction in the given log
   * @throws IOException if no such segment exists
   */
  @AtMostOnce
  public void finalizeLogSegment(RequestInfo reqInfo,
      long startTxId, long endTxId) throws IOException;
}
