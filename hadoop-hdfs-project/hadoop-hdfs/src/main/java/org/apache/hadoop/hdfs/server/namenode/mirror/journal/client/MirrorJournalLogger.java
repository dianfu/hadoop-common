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
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.NameNodeProxies;

import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.JournalState;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocol;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;

/**
 * logger to a remote journal using Hadoop IPC.
 */
@InterfaceAudience.Private
public class MirrorJournalLogger implements MirrorLogger {

  private final Configuration conf;
  protected final URI addr;
  private String regionId;
  private MirrorJournalProtocol proxy;

  private long ipcSerial = 0;

  private final String journalId;

  private final MirrorJournalLoggerMetrics metrics;

  /**
   * The number of bytes of edits data still in the queue.
   */
  private int queuedEditsSizeBytes = 0;

  /**
   * The highest txid that has been successfully logged on the remote JN.
   */
  private long highestAckedTxId = 0;

  /**
   * The maximum number of bytes that can be pending in the queue.
   * This keeps the writer from hitting OOME if one of the loggers
   * starts responding really slowly. Eventually, the queue
   * overflows and it starts to treat the logger as having errored.
   */
  private final int queueSizeLimitBytes;

  private static final long WARN_JOURNAL_MILLIS_THRESHOLD = 1000;

  static final Factory FACTORY = new MirrorLogger.Factory() {
    @Override
    public MirrorLogger createLogger(Configuration conf,
        String journalId, URI addr, String regionId) {
      return new MirrorJournalLogger(conf, journalId, addr, regionId);
    }
  };

  public MirrorJournalLogger(Configuration conf,
      String journalId, URI addr, String regionId) {
    this.conf = conf;
    this.journalId = journalId;
    this.addr = addr;
    this.regionId = regionId;

    this.queueSizeLimitBytes = 1024 * 1024 * conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_DEFAULT);

    metrics = MirrorJournalLoggerMetrics.create(this);
  }

  @Override
  public void close() {
    MirrorJournalManager.LOG.info("Closing", new Exception());
    // No more tasks may be submitted after this point.

    if (proxy != null) {
      // TODO: this can hang for quite some time if the client
      // is currently in the middle of a call to a downed JN.
      // We should instead do this asynchronously, and just stop
      // making any more calls after this point (eg clear the queue)
      RPC.stopProxy(proxy);
    }
  }

  protected MirrorJournalProtocol getProxy() throws IOException {
    if (proxy != null) return proxy;
    proxy = createProxy();
    return proxy;
  }

  protected MirrorJournalProtocol createProxy() throws IOException {
    final Configuration confCopy = new Configuration(conf);

    // Need to set NODELAY or else batches larger than MTU can trigger
    // 40ms nagling delays.
    confCopy.setBoolean(
        CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
        true);

    return SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<MirrorJournalProtocol>() {
          @Override
          public MirrorJournalProtocol run() throws IOException {
            NameNodeProxies.ProxyAndInfo<MirrorJournalProtocol> proxyInfo =
                NameNodeProxies.createProxy(confCopy, addr,
                    MirrorJournalProtocol.class, regionId);
            return proxyInfo.getProxy();
          }
        });
  }

  /**
   * Separated out for easy overriding in tests.
   */
  @VisibleForTesting
  protected ExecutorService createExecutor() {
    return Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("Logger channel to " + addr)
          .setUncaughtExceptionHandler(
              UncaughtExceptionHandlers.systemExit())
          .build());
  }

  private synchronized RequestInfo createReqInfo() {
    return new RequestInfo(journalId, ipcSerial++);
  }

  @VisibleForTesting
  synchronized long getNextIpcSerial() {
    return ipcSerial;
  }

  public synchronized int getQueuedEditsSize() {
    return queuedEditsSizeBytes;
  }

  public URI getRemoteAddress() {
    return addr;
  }

  @Override
  public GetJournalStateResponseProto getJournalState(boolean resetIPCSerial,
      NamespaceInfo nsInfo) throws IOException {
     JournalState js = getProxy().getJournalState(resetIPCSerial, nsInfo);
    return GetJournalStateResponseProto.newBuilder()
        .setLastCommittedTxId(js.getLastCommittedTxId())
        .build();
  }

  @Override
  public void sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data)  throws IOException {
    try {
      reserveQueueSpace(data.length);
    } catch (LoggerTooFarBehindException e) {
      throw new IOException(e);
    }

    // When this batch is acked, we use its submission time in order
    // to calculate how far we are lagging.
    try {
      long rpcSendTimeNanos = Time.monotonicNow();
      try {
        getProxy().journal(createReqInfo(),
            segmentTxId, firstTxnId, numTxns, data);
      } catch (IOException e) {
        MirrorJournalManager.LOG.warn(
            "Remote journal " + MirrorJournalLogger.this + " failed to " +
                "write txns " + firstTxnId + "-" + (firstTxnId + numTxns - 1) +
                ". Will try to write to this journal again after the next " +
                "log roll.", e);
        throw e;
      } finally {
        long now = Time.monotonicNow();
        long rpcTime = TimeUnit.MICROSECONDS.convert(
            now - rpcSendTimeNanos, TimeUnit.NANOSECONDS);
        metrics.addWriteRpcLatency(rpcTime);
        if (rpcTime / 1000 > WARN_JOURNAL_MILLIS_THRESHOLD) {
          MirrorJournalManager.LOG.warn(
              "Took " + (rpcTime / 1000) + "ms to send a batch of " +
                  numTxns + " edits (" + data.length + " bytes) to " +
                  "remote journal " + MirrorJournalLogger.this);
        }
      }
      synchronized (MirrorJournalLogger.this) {
        highestAckedTxId = firstTxnId + numTxns - 1;
      }
    } finally {
      // so adjust the queue size back down.
      unreserveQueueSpace(data.length);
    }
  }

  private synchronized void reserveQueueSpace(int size)
      throws LoggerTooFarBehindException {
    Preconditions.checkArgument(size >= 0);
    if (queuedEditsSizeBytes + size > queueSizeLimitBytes &&
        queuedEditsSizeBytes > 0) {
      throw new LoggerTooFarBehindException();
    }
    queuedEditsSizeBytes += size;
  }

  private synchronized void unreserveQueueSpace(int size) {
    Preconditions.checkArgument(size >= 0);
    queuedEditsSizeBytes -= size;
  }

  @Override
  public void startLogSegment(long txid, int layoutVersion) throws IOException {
    getProxy().startLogSegment(createReqInfo(), txid, layoutVersion);
  }

  @Override
  public void finalizeLogSegment (
      long startTxId, long endTxId) throws IOException {  
    getProxy().finalizeLogSegment(createReqInfo(), startTxId, endTxId);
  }

  @Override
  public String toString() {
    return addr.getHost() + ':' + addr.getPort();
  }

  @Override
  public synchronized void appendReport(StringBuilder sb) {
    sb.append("Written txid ").append(highestAckedTxId);
  }
}
