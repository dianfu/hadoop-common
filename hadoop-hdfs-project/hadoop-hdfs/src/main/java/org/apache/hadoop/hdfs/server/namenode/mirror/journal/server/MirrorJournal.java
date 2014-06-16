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
package org.apache.hadoop.hdfs.server.namenode.mirror.journal.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.JournalSegmentNotOpenedException;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.JournalState;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.RequestInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;

import com.google.common.base.Stopwatch;

public class MirrorJournal implements Closeable {
  static final Log LOG = LogFactory.getLog(MirrorJournal.class);
  private MirrorJournalSet mirrorJournalSet;

  // Current writing state
  private EditLogOutputStream curSegment;
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;
  private long nextTxId = HdfsConstants.INVALID_TXID;
  private long highestWrittenTxId = -1;

  private final String journalId;

  /**
   * Each IPC that comes from a given client contains a serial number which only
   * increases from the client's perspective. Whenever an IPC comes from a client,
   * we ensure that it is strictly higher than any previous IPC. This guards
   * against any bugs in the IPC layer that would re-order IPCs or cause a stale
   * retry from an old request to resurface and confuse things.
   */
  private long currentIpcSerial = -1;

  private final MirrorJournalMetrics metrics;

  /**
   * Time threshold for sync calls, beyond which a warning should be logged to
   * the console.
   */
  private static final int WARN_SYNC_MILLIS_THRESHOLD = 1000;

  public MirrorJournal(Configuration conf, String journalId,
      FSNamesystem namesystem) {
    this.journalId = journalId;
    this.metrics = MirrorJournalMetrics.create(this);
    highestWrittenTxId = namesystem.getLastWrittenTransactionId();
    // initialize mirror journal set
    mirrorJournalSet = new MirrorJournalSet(namesystem);
  }

  /**
   * Unlock and release resources.
   */
  @Override
  public void close() throws IOException {
    IOUtils.closeStream(curSegment);
  }

  String getJournalId() {
    return journalId;
  }

  public synchronized long getHighestWrittenTxId() {
    return highestWrittenTxId;
  }

  public JournalState getJournalState(boolean resetIPCSerial) {
    if (resetIPCSerial) {
      currentIpcSerial = -1;
    }
    return new JournalState(highestWrittenTxId);
  }

  private void abortCurSegment() throws IOException {
    if (curSegment == null) {
      return;
    }

    curSegment.abort();
    curSegment = null;
    curSegmentTxId = HdfsConstants.INVALID_TXID;
  }

  public long getCurSegmentTxId() throws IOException {
    return curSegmentTxId;
  }

  /**
   * Write a batch of edits to the journal. {@see
   * QJournalProtocol#journal(RequestInfo, long, long, int, byte[])}
   */
  public synchronized void journal(RequestInfo reqInfo, long segmentTxId,
      long firstTxnId, int numTxns, byte[] records) throws IOException {
    checkSegmentOpened();

    if (curSegmentTxId != segmentTxId) {
      // Sanity check: it is possible that the writer will fail IPCs
      // on both the finalize() and then the start() of the next segment.
      // This could cause us to continue writing to an old segment
      // instead of rolling to a new one, which breaks one of the
      // invariants in the design. If it happens, abort the segment
      // and throw an exception.
      JournalOutOfSyncException e = new JournalOutOfSyncException(
          "Writer out of sync: it thinks it is writing segment " + segmentTxId
              + " but current segment is " + curSegmentTxId);
      abortCurSegment();
      throw e;
    }

    checkSync(nextTxId == firstTxnId, "Can't write txid " + firstTxnId
        + " expecting nextTxId=" + nextTxId);

    long lastTxnId = firstTxnId + numTxns - 1;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writing txid " + firstTxnId + "-" + lastTxnId);
    }

    curSegment.writeRaw(records, 0, records.length);
    curSegment.setReadyToFlush();
    Stopwatch sw = new Stopwatch();
    sw.start();
    curSegment.flush(true);
    sw.stop();

    metrics.addSync(sw.elapsedTime(TimeUnit.MICROSECONDS));
    if (sw.elapsedTime(TimeUnit.MILLISECONDS) > WARN_SYNC_MILLIS_THRESHOLD) {
      LOG.warn("Sync of transaction range " + firstTxnId + "-" + lastTxnId
          + " took " + sw.elapsedTime(TimeUnit.MILLISECONDS) + "ms");
    }

    metrics.batchesWritten.incr(1);
    metrics.bytesWritten.incr(records.length);
    metrics.txnsWritten.incr(numTxns);

    highestWrittenTxId = lastTxnId;
    nextTxId = lastTxnId + 1;
  }

  /**
   * Ensure that the given request is coming from the correct writer and
   * in-order.
   * 
   * @param reqInfo
   *          the request info
   * @throws IOException
   *           if the request is invalid.
   */
  private synchronized void checkRequest(RequestInfo reqInfo)
      throws IOException {
    // Ensure that the IPCs are arriving in-order as expected.
    checkSync(reqInfo.getIpcSerialNumber() > currentIpcSerial,
        "IPC serial %s from client %s was not higher than prior highest "
            + "IPC serial %s", reqInfo.getIpcSerialNumber(),
        Server.getRemoteIp(), currentIpcSerial);
    currentIpcSerial = reqInfo.getIpcSerialNumber();
  }

  /**
   * @throws JournalOutOfSyncException
   *           if the given expression is not true. The message of the exception
   *           is formatted using the 'msg' and 'formatArgs' parameters.
   */
  private void checkSync(boolean expression, String msg, Object... formatArgs)
      throws JournalOutOfSyncException {
    if (!expression) {
      throw new JournalOutOfSyncException(String.format(msg, formatArgs));
    }
  }

  private void checkSegmentOpened() throws JournalSegmentNotOpenedException {
    if (curSegment == null) {
      throw new JournalSegmentNotOpenedException("Can't write, no segment open");
    }
  }

  /**
   * Start a new segment at the given txid. The previous segment must have
   * already been finalized.
   */
  public synchronized void startLogSegment(RequestInfo reqInfo,
      long txid, int layoutVersion) throws IOException {
    checkRequest(reqInfo);

    if (curSegment != null) {
      LOG.warn("Client is requesting a new log segment " + txid
          + " though we are already writing " + curSegment + ". "
          + "Aborting the current segment in order to begin the new one.");
      // The writer may have lost a connection to us and is now
      // re-connecting after the connection came back.
      // We should abort our own old segment.
      abortCurSegment();
    }

    curSegment = mirrorJournalSet.startLogSegment(txid, layoutVersion);
    curSegmentTxId = txid;
    nextTxId = txid;
  }

  /**
   * Finalize the log segment at the given transaction ID.
   */
  public synchronized void finalizeLogSegment(RequestInfo reqInfo,
      long startTxId, long endTxId) throws IOException {
    checkRequest(reqInfo);

    // Finalizing the log that the writer was just writing.
    if (startTxId == curSegmentTxId) {
      if (curSegment != null) {
        // curSegment.close();
        curSegment = null;
        curSegmentTxId = HdfsConstants.INVALID_TXID;
      }

      checkSync(nextTxId == endTxId + 1,
          "Trying to finalize in-progress log segment %s to end at "
              + "txid %s but only written up to txid %s", startTxId, endTxId,
          nextTxId - 1);
    }

    mirrorJournalSet.finalizeLogSegment(startTxId, endTxId);
  }
}
