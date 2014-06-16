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
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.namenode.mirror.MirrorManager.MirrorOperationCategory;

/**
 * Base state to implement state machine pattern for state transition of mirror
 */
@InterfaceAudience.Private
abstract public class ClusterState {

  /**
   * An Mirror service may be in primary or mirror state. During startup, it is in
   * an unknown INITIALIZING state. During shutdown, it is in the STOPPING state
   * and can no longer return to primary/mirror states.
   */
  public enum MirrorServiceState {
    INITIALIZING("initializing"),
    PRIMARY("primary"),
    MIRROR("mirror"),
    STOPPING("stopping");

    private String name;

    MirrorServiceState(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  protected final MirrorServiceState state;
  
  /**
   * Constructor
   * @param name Name of the state.
   */
  public ClusterState(MirrorServiceState state) {
    this.state = state;
  }

  /**
   * @return the generic service state
   */
  public MirrorServiceState getServiceState() {
    return state;
  }

  /**
   * Internal method to transition the state of a given node to a new state.
   * @param context the mirror context
   * @param s new state
   * @throws IOException on failure to transition to new state.
   */
  protected final void setStateInternal(final MirrorContext context, final ClusterState s)
      throws IOException {
    prepareToExitState(context);
    s.prepareToEnterState(context);
    context.writeLock();
    try {
      exitState(context);
      context.setState(s);
      s.enterState(context);
    } finally {
      context.writeUnlock();
    }
  }

  /**
   * Method to be overridden by subclasses to prepare to enter a state.
   * This method is called <em>without</em> the context being locked,
   * and after {@link #prepareToExitState(MirrorContext)} has been called
   * for the previous state, but before {@link #exitState(MirrorContext)}
   * has been called for the previous state.
   * @param context the mirror context
   * @throws IOException on precondition failure
   */
  public void prepareToEnterState(final MirrorContext context)
      throws IOException {}

  /**
   * Method to be overridden by subclasses to perform steps necessary for
   * entering a state.
   * @param context the mirror context
   * @throws IOException on failure to enter the state.
   */
  public abstract void enterState(final MirrorContext context)
      throws IOException;

  /**
   * Method to be overridden by subclasses to prepare to exit a state.
   * This method is called <em>without</em> the context being locked.
   * This is used by the standby state to cancel any checkpoints
   * that are going on. It can also be used to check any preconditions
   * for the state transition.
   * 
   * This method should not make any destructuve changes to the state
   * (eg stopping threads) since {@link #prepareToEnterState(MirrorContext)}
   * may subsequently cancel the state transition.
   * @param context the mirror context
   * @throws IOException on precondition failure
   */
  public void prepareToExitState(final MirrorContext context)
      throws IOException {}

  /**
   * Method to be overridden by subclasses to perform steps necessary for
   * exiting a state.
   * @param context the mirror context
   * @throws IOException on failure to enter the state.
   */
  public abstract void exitState(final MirrorContext context)
      throws IOException;

  /**
   * Move from the existing state to a new state
   * @param context the mirror context
   * @param s new state
   * @throws IOException on failure to transition to new state.
   */
  public void setState(MirrorContext context, ClusterState s) throws IOException {
    if (this == s) { // Aleady in the new state
      return;
    }
    throw new IOException("Transtion from state " + this + " to "
        + s + " is not allowed.");
  }
  
  /**
   * Check if an operation is supported in a given state.
   * @param context the mirror context
   * @param op Type of the operation.
   * @throws UnsupportedActionException if a given type of operation is not
   *           supported in this state.
   */
  public abstract void checkOperation(final MirrorContext context, final MirrorOperationCategory op)
      throws IOException;

  public abstract boolean shouldPopulateReplQueues();

  /**
   * @return String representation of the service state.
   */
  @Override
  public String toString() {
    return state.toString();
  }
}
