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
 * Context that is to be used by {@link ClusterState} for getting/setting the
 * current state and performing required operations.
 */
@InterfaceAudience.Private
public interface MirrorContext {
  /** Set the state of the context to given {@code state} */
  public void setState(ClusterState state);
  
  /** Get the state from the context */
  public ClusterState getState();
  
  /** Start the services required in primary state */
  public void startPrimaryServices() throws IOException;
  
  /** Stop the services when exiting primary state */
  public void stopPrimaryServices() throws IOException;
  
  /** Start the services required in mirror state */
  public void startMirrorServices() throws IOException;

  /** Prepare to exit the mirror state */
  public void prepareToStopMirrorServices() throws IOException;

  /** Stop the services when exiting mirror state */
  public void stopMirrorServices() throws IOException;

  /**
   * Take a write-lock on the underlying system
   * so that no concurrent state transitions or edits
   * can be made.
   */
  void writeLock();

  /**
   * Unlock the lock taken by {@link #writeLock()}
   */
  void writeUnlock();

  /**
   * Verify that the given operation category is allowed in the current state.
   */
  void checkOperation(MirrorOperationCategory op) throws IOException;
}
