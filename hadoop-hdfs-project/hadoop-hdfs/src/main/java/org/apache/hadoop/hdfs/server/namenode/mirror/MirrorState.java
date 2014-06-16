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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.mirror.MirrorManager.MirrorOperationCategory;

/**
 * Mirror state of the active namenode of mirror cluster. In this state, namenode
 * provides the namenode service and handles:
 * <ul>
 * <li>Receiving or tailing edits from primary cluster.</li>
 * <li>Heartbeat for sharing network topology and cluster status to primary cluster.</li>
 * <li>Schedule replication request to primary cluster.</li>
 * </ul>
 * 
 */
@InterfaceAudience.Private
public class MirrorState extends ClusterState {
  public MirrorState() {
    super(MirrorServiceState.MIRROR);
  }

  @Override
  public void setState(MirrorContext context, ClusterState s) throws IOException {
    if (s == MirrorManager.PRIMARY_STATE) {
      setStateInternal(context, s);
      return;
    }
    super.setState(context, s);
  }

  @Override
  public void enterState(MirrorContext context) throws IOException {
    try {
      context.startMirrorServices();
    } catch (IOException e) {
      throw new IOException("Failed to start mirror services", e);
    }
  }

  @Override
  public void prepareToExitState(MirrorContext context) throws IOException {
    context.prepareToStopMirrorServices();
  }

  @Override
  public void exitState(MirrorContext context) throws IOException {
    try {
      context.stopMirrorServices();
    } catch (IOException e) {
      throw new IOException("Failed to stop mirror services", e);
    }
  }

  @Override
  public void checkOperation(MirrorContext context, MirrorOperationCategory op)
      throws IOException {
    if (op == MirrorOperationCategory.UNCHECKED ||
        (op == MirrorOperationCategory.MIRROR_JOURNAL) || 
        (op == MirrorOperationCategory.MIRROR)) {
      return;
    }
    String msg = "Operation category " + op + " is not supported in state "
        + context.getState();
    throw new MirrorStateException(msg);
  }

  @Override
  public boolean shouldPopulateReplQueues() {
    return false;
  }
}

