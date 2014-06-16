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
package org.apache.hadoop.hdfs.server.namenode.mirror;

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.util.ExitUtil.ExitException;

/**
 * Keeps information and manage the cluster state related to mirror feature
 */
@InterfaceAudience.Private
public class MirrorManager {
  static final Log LOG = LogFactory.getLog(MirrorManager.class);

  public static final String MIRROR_JOURNAL_IDENTIFIER = "mirror";

  public static final ClusterState PRIMARY_STATE = new PrimaryState();
  public static final ClusterState MIRROR_STATE = new MirrorState();

  private FSNamesystem namesystem;
  private Configuration conf;
  private volatile ClusterState state;
  private MirrorContext mirrorContext;

  /**
   * Categories of mirror operations supported by the namenode.
   */
  public static enum MirrorOperationCategory {
    /** Operations that are state agnostic */
    UNCHECKED,
    /** Operations related to {@link MirrorJournalProtocol} */
    MIRROR_JOURNAL,
    /** Operations related to primary operations */
    PRIMARY,
    /** Operations related to other mirror operation */
    MIRROR
  }

  public MirrorManager(FSNamesystem namesystem,
      final Configuration conf) throws IOException {
    this.namesystem = namesystem;
    this.conf = conf;
    this.mirrorContext = createMirrorContext();
  }

  public void start() throws IOException {
    this.state = createClusterState();
    
    try {
      mirrorContext.writeLock();
      state.prepareToEnterState(mirrorContext);
      state.enterState(mirrorContext);
    } finally {
      mirrorContext.writeUnlock();
    }
  }

  public void stop() {
    try {
      mirrorContext.writeLock();
      if(state != null) {
        state.prepareToExitState(mirrorContext);
        state.exitState(mirrorContext);
        state = null;
      }
    } catch(IOException e) {
      LOG.error("Failed to stop mirror services.", e);
    } finally {
      mirrorContext.writeUnlock();
    }
  }

  public void checkMirrorJournalOperation() throws IOException {
    if (mirrorContext != null) {
      mirrorContext.checkOperation(MirrorOperationCategory.MIRROR_JOURNAL);
    }
  }

  public void checkPrimaryOperation() throws IOException {
    if (mirrorContext != null) {
      mirrorContext.checkOperation(MirrorOperationCategory.PRIMARY);
    }
  }

  public void checkMirrorOperation() throws IOException {
    if (mirrorContext != null) {
      mirrorContext.checkOperation(MirrorOperationCategory.MIRROR);
    }
  }

  public void startPrimaryServices() {
  }

  public void stopPrimaryServices() {
  }

  public void startMirrorServices() {
  }

  public void stopMirrorServices() {
  }

  public void prepareToStopMirrorServices() {
  }

  protected ClusterState createClusterState() {
    // currently static decide whether we are primary or mirror cluster
    // to be improved in the feature
    return isPrimaryCluster() ? PRIMARY_STATE : MIRROR_STATE;
  }

  public boolean isPrimaryCluster() {
    return MirrorUtil.isPrimaryCluster(conf);
  }

  protected MirrorContext createMirrorContext() {
    return new MirrorManagerContext();
  }

  /**
   * Shutdown the NN immediately in an ungraceful way. Used when it
   * would be unsafe for the NN to continue operating.
   * 
   * @param t exception which warrants the shutdown. Printed to the
   *          NN log before exit.
   * @throws ExitException thrown only for testing.
   */
  protected synchronized void doImmediateShutdown(Throwable t)
      throws ExitException {
    String message = "Error encountered requiring NN shutdown. " +
        "Shutting down immediately.";
    try {
      LOG.fatal(message, t);
    } catch (Throwable ignored) {
      // This is unlikely to happen, but there's nothing we can do if it does.
    }
    terminate(1, t);
  }

  /**
   * Class used to expose {@link MirrorManager} as context to {@link MirrorContext}
   */
  protected class MirrorManagerContext implements MirrorContext {
    @Override
    public void setState(ClusterState s) {
      state = s;
    }

    @Override
    public ClusterState getState() {
      return state;
    }

    @Override
    public void startPrimaryServices() throws IOException {
      try {
        namesystem.startPrimaryServices();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void stopPrimaryServices() throws IOException {
      try {
        namesystem.stopPrimaryServices();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void startMirrorServices() throws IOException{
      try {
        namesystem.startMirrorServices();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void prepareToStopMirrorServices() throws IOException {
      try {
        namesystem.prepareToStopMirrorServices();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void stopMirrorServices() throws IOException {
      try {
        namesystem.stopMirrorServices();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void writeLock() {
      namesystem.writeLock();
    }

    @Override
    public void writeUnlock() {
      namesystem.writeUnlock();
    }

    /** Check if an operation of given category is allowed */
    @Override
    public void checkOperation(final MirrorOperationCategory op)
        throws IOException {
      state.checkOperation(mirrorContext, op);
    }
  }
}
