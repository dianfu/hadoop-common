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
 * Primary state of the active namenode of primary cluster. In this state, namenode
 * provides the namenode service and handles:
 * <ul>
 * <li>write namespace journal to mirror clusters.</li>
 * <li>handles network topology and cluster status sharing mirror clusters.</li>
 * <li>handles sync data block pipeline to mirror clusters.</li>
 * <li>handles block replication request from mirror clusters.</li>
 * </ul>
 * 
 */
@InterfaceAudience.Private
public class PrimaryState extends ClusterState {
  public PrimaryState() {
    super(MirrorServiceState.PRIMARY);
  }

  @Override
  public void checkOperation(MirrorContext context, MirrorOperationCategory op) 
      throws IOException {
    if (op == MirrorOperationCategory.MIRROR_JOURNAL) {
      String msg = "Operation category " + op + " is not supported in state "
          + context.getState();
      throw new PrimaryStateException(msg);
    }
  }

  @Override
  public boolean shouldPopulateReplQueues() {
    return true;
  }

  @Override
  public void setState(MirrorContext context, ClusterState s)
      throws IOException {
    if (s == MirrorManager.MIRROR_STATE) {
      setStateInternal(context, s);
      return;
    }
    super.setState(context, s);
  }

  @Override
  public void enterState(MirrorContext context) throws IOException {
    try {
      context.startPrimaryServices();
    } catch (IOException e) {
      throw new IOException("Failed to start primary services", e);
    }
  }

  @Override
  public void exitState(MirrorContext context) throws IOException {
    try {
      context.stopPrimaryServices();
    } catch (IOException e) {
      throw new IOException("Failed to stop primary services", e);
    }
  }

}
