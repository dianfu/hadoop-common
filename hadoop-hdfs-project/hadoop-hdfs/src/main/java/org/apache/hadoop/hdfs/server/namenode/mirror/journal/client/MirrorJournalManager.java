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
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditsDoubleBuffer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.mirror.MirrorManager;
import org.apache.hadoop.hdfs.server.namenode.mirror.MirrorUtil;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.JournalSegmentNotOpenedException;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.base.Preconditions;

/**
 * A JournalManager that writes to mirror active namenode
 */
@InterfaceAudience.Private
public class MirrorJournalManager implements JournalManager {
  static final Log LOG = LogFactory.getLog(MirrorJournalManager.class);

  private final Configuration conf;
  private final String regionId;
  private boolean isActiveWriter;
  private NamespaceInfo nsInfo;

  private FSEditLog editLog;

  private final MirrorLogger logger;

  private int outputBufferCapacity = 512 * 1024;

  private long lastCommittedTxId = -1;
  private boolean editLogSyncProcessed = false;

  public MirrorJournalManager(Configuration conf,
      String regionId, NamespaceInfo nsInfo, FSEditLog editLog) {
    this(conf, regionId, nsInfo, editLog, MirrorJournalLogger.FACTORY);
  }

  MirrorJournalManager(Configuration conf,
      String regionId, NamespaceInfo nsInfo, FSEditLog editLog,
      MirrorLogger.Factory loggerFactory) {
    Preconditions.checkArgument(conf != null, "must be configured");

    this.conf = conf;
    this.regionId = regionId;
    this.editLog = editLog;
    this.logger = createLogger(loggerFactory);
    this.nsInfo = nsInfo;
  }

  protected MirrorLogger createLogger(MirrorLogger.Factory factory) {
    return createLogger(conf, regionId, factory);
  }

  @Override
  public void format(NamespaceInfo nsInfo) throws IOException {
    // the format operation of mirror cluster should
    // be done by the mirror cluster itself.
  }

  @Override
  public boolean hasSomeData() throws IOException {
    // the format operation of mirror cluster should
    // be done by the mirror cluster itself.
    return false;
  }

  static MirrorLogger createLogger(Configuration conf,
      String regionId, MirrorLogger.Factory factory) {
    URI addr = MirrorUtil.getMirrorLoggerAddress(conf, regionId);
    return factory.createLogger(conf, 
        MirrorManager.MIRROR_JOURNAL_IDENTIFIER, addr, regionId);
  }

  @Override
  public EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException {
    Preconditions.checkState(isActiveWriter,
        "must recover segments before starting a new one");
    if (txId <= lastCommittedTxId) {
      throw new IOException("Mirror Cluster at Region ["
          + regionId + "] is newer than the Primary Cluster.");
    } else if (txId > lastCommittedTxId + 1 && !editLogSyncProcessed) {
      long endTxId = syncLogsFrom(lastCommittedTxId + 1);
      editLogSyncProcessed = true;
      Preconditions.checkState(endTxId + 1 == txId,
          "There is gap between last synced transaction ID and the new transaction ID");
    }

    logger.startLogSegment(txId, layoutVersion);
    return new MirrorLoggerOutputStream(logger, txId,
        outputBufferCapacity, layoutVersion);
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    logger.finalizeLogSegment(firstTxId, lastTxId);
  }

  @Override
  public void setOutputBufferCapacity(int size) {
    outputBufferCapacity = size;
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    // The removal of older journal log should be done by the mirror cluster
    // itself, should not be triggered by primary cluster
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    Preconditions.checkState(!isActiveWriter, "already active writer");

    LOG.info("Starting recovery process for unclosed journal segments...");
    GetJournalStateResponseProto resps = logger.getJournalState(true, nsInfo);

    if (LOG.isDebugEnabled()) {
      LOG.debug("getJournalState(" + resps.getLastCommittedTxId() + ") response:\n" +
        resps);
    }

    lastCommittedTxId = Math.max(Long.MIN_VALUE, resps.getLastCommittedTxId());

    isActiveWriter = true;
  }

  private long syncLogsFrom(long fromTxId) throws IOException {
    return editLog.syncLogsFrom(fromTxId, this);
  }

  @Override
  public void close() throws IOException {
    logger.close();
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk) throws IOException {
    throw new IOException("Operation is not supported.");
  }

  @Override
  public String toString() {
    return "Mirror Journal Manager to " + logger;
  }

  /**
   * EditLogOutputStream implementation that writes to a mirror journal.
   */
  class MirrorLoggerOutputStream extends EditLogOutputStream {
    private final MirrorLogger logger;
    private EditsDoubleBuffer buf;
    private long segmentTxId;
    private int layoutVersion;

    public MirrorLoggerOutputStream(MirrorLogger logger,
        long txId, int outputBufferCapacity, int layoutVersion) throws IOException {
      super();
      this.buf = new EditsDoubleBuffer(outputBufferCapacity);
      this.logger = logger;
      this.segmentTxId = txId;
      this.layoutVersion = layoutVersion;
    }

    @Override
    public void write(FSEditLogOp op) throws IOException {
      buf.writeOp(op);
    }

    @Override
    public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
      buf.writeRaw(bytes, offset, length);
    }

    @Override
    public void create(int layoutVersion) throws IOException {
      throw new IOException("Operation is not supported.");
    }

    @Override
    public void close() throws IOException {
      if (buf != null) {
        buf.close();
        buf = null;
      }
    }

    @Override
    public void abort() throws IOException {
      MirrorJournalManager.LOG.warn("Aborting " + this);
      buf = null;
      close();
    }

    @Override
    public void setReadyToFlush() throws IOException {
      buf.setReadyToFlush();
    }

    @Override
    public boolean shouldForceSync() {
      return true;
    }

    @Override
    protected void flushAndSync(boolean durable) throws IOException {
      int numReadyBytes = buf.countReadyBytes();
      if (numReadyBytes > 0) {
        int numReadyTxns = buf.countReadyTxns();
        long firstTxToFlush = buf.getFirstReadyTxId();

        assert numReadyTxns > 0;

        DataOutputBuffer bufToSend = new DataOutputBuffer(numReadyBytes);
        buf.flushTo(bufToSend);
        assert bufToSend.getLength() == numReadyBytes;
        byte[] data = bufToSend.getData();
        assert data.length == bufToSend.getLength();

        while (true) {
          try {
            logger.sendEdits(
                segmentTxId, firstTxToFlush,
                numReadyTxns, data);
            lastCommittedTxId += numReadyTxns;
            break;
          } catch (RemoteException e) {
            // This case will happen when mirror cluster restart
            if (e.unwrapRemoteException() instanceof JournalSegmentNotOpenedException) {
              logger.startLogSegment(firstTxToFlush, layoutVersion);
              segmentTxId = firstTxToFlush;
              continue;
            } else {
              throw e;
            }
          }
        }
      }
    }

    @Override
    public String generateReport() {
      StringBuilder sb = new StringBuilder();
      sb.append("Writing segment beginning at txid " + segmentTxId + ". \n");
      logger.appendReport(sb);
      return sb.toString();
    }

    @Override
    public String toString() {
      return "MirrorJournalOutputStream starting at txid " + segmentTxId;
    }
  }

  @Override
  public void doPreUpgrade() throws IOException {
    throw new IOException("Operation is not supported.");
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    throw new IOException("Operation is not supported.");
  }

  @Override
  public void doFinalize() throws IOException {
    throw new IOException("Operation is not supported.");
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    return false;
  }

  @Override
  public void doRollback() throws IOException {
    throw new IOException("Operation is not supported.");
  }

  @Override
  public void discardSegments(long startTxId) throws IOException {
    throw new IOException("Operation is not supported.");
  }

  @Override
  public long getJournalCTime() throws IOException {
    return 0;
  }
}
