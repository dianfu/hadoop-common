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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditsDoubleBuffer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * Manages the Journals of mirror cluster through FSEditLog.
 */
@InterfaceAudience.Private
public class MirrorJournalSet implements JournalManager {
  static final Log LOG = LogFactory.getLog(MirrorJournalSet.class);

  private final FSNamesystem namesystem;
  private int outputBufferCapacity = 512*1024;
  private int layoutVersion;

  MirrorJournalSet(FSNamesystem namesystem) {
    this.namesystem = namesystem;
  }

  @Override
  public void format(NamespaceInfo nsInfo) throws IOException {
    // The iteration is done by FSEditLog itself
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasSomeData() throws IOException {
    // This is called individually on the underlying journals,
    // not on the JournalSet.
    throw new UnsupportedOperationException();
  }

  @Override
  public EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException {
    this.layoutVersion = layoutVersion;
    namesystem.getEditLog().startLogSegment(txId, layoutVersion, true);
    return new MirrorJournalOutputStream();
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    namesystem.getEditLog().endCurrentLogSegment(false);
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void purgeLogsOlderThan(final long minTxIdToKeep) throws IOException {
    namesystem.getEditLog().purgeLogsOlderThan(minTxIdToKeep);
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOutputBufferCapacity(final int size) {
    namesystem.getEditLog().setOutputBufferCapacity(size);
    setOutputBufferCapacityInternal(size);
  }

  synchronized void setOutputBufferCapacityInternal(int size) {
    this.outputBufferCapacity = size;
  }

  private void applyEdits(byte[] edits, int offset, int length) throws IOException {
    List<EditLogInputStream> editLogStreams = new ArrayList<EditLogInputStream>();
    MirrorJournalInputStream editLogStream = new MirrorJournalInputStream(
        HdfsConstants.INVALID_TXID, HdfsConstants.INVALID_TXID,
        edits, offset, length);
    editLogStreams.add(editLogStream);

    FSImage fsImage = namesystem.getFSImage();
    fsImage.loadEdits(editLogStreams, namesystem);
  }

  @Override
  public void doPreUpgrade() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doFinalize() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doRollback() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void discardSegments(long startTxId) throws IOException {
    namesystem.getEditLog().discardSegments(startTxId);
  }

  @Override
  public long getJournalCTime() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * EditLogOutputStream implementation that writes to a mirror journal.
   */
  class MirrorJournalOutputStream extends EditLogOutputStream {
    private EditsDoubleBuffer doubleBuf;

    MirrorJournalOutputStream() throws IOException {
      super();

      doubleBuf = new EditsDoubleBuffer(outputBufferCapacity);
    }

    @Override
    public void write(final FSEditLogOp op)
        throws IOException {
      namesystem.getEditLog().logEdit(op);
      // write to buffer
      doubleBuf.writeOp(op);
    }

    @Override
    public void writeRaw(final byte[] data, final int offset, final int length)
        throws IOException {
      namesystem.getEditLog().logEdit(length, 
          Arrays.copyOfRange(data, offset, offset + length));
      // write to buffer
      doubleBuf.writeRaw(data, offset, length);
    }

    @Override
    public void create(int layoutVersion) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      namesystem.getEditLog().close();

      closeInternal();
    }

    @Override
    public void abort() throws IOException {
      namesystem.getEditLog().abortCurrentLogSegment();
    }

    @Override
    public void setReadyToFlush() throws IOException {
      namesystem.getEditLog().setReadyToFlush();

      setReadyToFlushInternal();
    }

    @Override
    protected void flushAndSync(final boolean durable) throws IOException {
      namesystem.getEditLog().flushAndSync(durable);

      flushAndSyncInternal();
    }

    @Override
    public void flush() throws IOException {
      namesystem.getEditLog().flush();
    }

    @Override
    public boolean shouldForceSync() {
      if (namesystem.getEditLog().shouldForceSync()) {
        return true;
      }

      if (shouldForceSyncInternal()) {
        return true;
      }

      return false;
    }

    @Override
    public long getNumSync() {
      throw new UnsupportedOperationException();
    }

    private void closeInternal() throws IOException {
      try {
        // close should have been called after all pending transactions
        // have been flushed & synced.
        // if already closed, just skip
        if (doubleBuf != null) {
          doubleBuf.close();
          doubleBuf = null;
        }
      } finally {
        doubleBuf = null;
      }
    }

    private void setReadyToFlushInternal() throws IOException {
      doubleBuf.setReadyToFlush();
    }

    private void flushAndSyncInternal() throws IOException {
      if (doubleBuf.isFlushed()) {
        LOG.info("Nothing to flush");
        return;
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      doubleBuf.flushTo(out);

      byte[] editsToSync = out.toByteArray();
      applyEdits(editsToSync, 0, editsToSync.length);
    }

    private boolean shouldForceSyncInternal() {
      return doubleBuf.shouldForceSync();
    }
  }

  private class MirrorJournalInputStream extends EditLogInputStream {
    private InputStream input;
    private long length;

    private final long firstTxId;
    private final long lastTxId;
    private FSEditLogOp.Reader reader;
    private FSEditLogLoader.PositionTrackingInputStream tracker;

    public MirrorJournalInputStream(long firstTxId, long lastTxId,
        byte[] data, int offset, int length) throws IOException {
      this.firstTxId = firstTxId;
      this.lastTxId = lastTxId;
      this.length = length;

      input = new ByteArrayInputStream(data, offset, length);
      tracker = new FSEditLogLoader.PositionTrackingInputStream(input);
      DataInputStream dataIn = new DataInputStream(tracker);

      reader = new FSEditLogOp.Reader(dataIn, tracker, layoutVersion);
    }

    @Override
    public long getFirstTxId() {
      return firstTxId;
    }

    @Override
    public long getLastTxId() {
      return lastTxId;
    }

    @Override
    public long length() throws IOException {
      return length;
    }

    @Override
    public long getPosition() {
      return tracker.getPos();
    }

    @Override
    protected FSEditLogOp nextOp() throws IOException {
      return reader.readOp(false);
    }

    @Override
    public int getVersion(boolean verifyVersion) throws IOException {
      return layoutVersion;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override
    public String getName() {
      return "MirrorJournalInputStream";
    }

    @Override
    public boolean isInProgress() {
      return true;
    }

    @Override
    public void setMaxOpSize(int maxOpSize) {
      reader.setMaxOpSize(maxOpSize);
    }
  }

}
