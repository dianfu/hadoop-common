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
package org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocolPB;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.JournalState;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocol;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.GetJournalStateRequestProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.JournalIdProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.RequestInfoProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link MirrorJournalProtocol} interfaces to the RPC server implementing
 * {@link MirrorJournalProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MirrorJournalProtocolTranslatorPB implements ProtocolMetaInterface,
    MirrorJournalProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final MirrorJournalProtocolPB rpcProxy;

  public MirrorJournalProtocolTranslatorPB(MirrorJournalProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public JournalState getJournalState(boolean resetIPCSerial,
      NamespaceInfo nsInfo) throws IOException {
    try {
      GetJournalStateRequestProto req = GetJournalStateRequestProto.newBuilder()
          .setResetIPCSerial(resetIPCSerial)
          .setNsInfo(PBHelper.convert(nsInfo))
          .build();
      return new JournalState(rpcProxy.getJournalState(NULL_CONTROLLER, req)
          .getLastCommittedTxId());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private JournalIdProto convertJournalId(String jid) {
    return JournalIdProto.newBuilder()
        .setIdentifier(jid)
        .build();
  }

  @Override
  public void journal(RequestInfo reqInfo,
      long segmentTxId, long firstTxnId, int numTxns,
      byte[] records) throws IOException {
    JournalRequestProto req = JournalRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setSegmentTxnId(segmentTxId)
        .setFirstTxnId(firstTxnId)
        .setNumTxns(numTxns)
        .setRecords(PBHelper.getByteString(records))
        .build();
    try {
      rpcProxy.journal(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private MirrorJournalProtocolProtos.RequestInfoProto convert(
      RequestInfo reqInfo) {
    RequestInfoProto.Builder builder = RequestInfoProto.newBuilder()
        .setJournalId(convertJournalId(reqInfo.getJournalId()))
        .setIpcSerialNumber(reqInfo.getIpcSerialNumber());
    return builder.build();
  }

  @Override
  public void startLogSegment(RequestInfo reqInfo, long txid, int layoutVersion)
      throws IOException {
    StartLogSegmentRequestProto req = StartLogSegmentRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setTxid(txid)
        .setLayoutVersion(layoutVersion)
        .build();
    try {
      rpcProxy.startLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    FinalizeLogSegmentRequestProto req =
        FinalizeLogSegmentRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setStartTxId(startTxId)
        .setEndTxId(endTxId)
        .build();
    try {
      rpcProxy.finalizeLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        MirrorJournalProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(MirrorJournalProtocolPB.class), methodName);
  }

}
