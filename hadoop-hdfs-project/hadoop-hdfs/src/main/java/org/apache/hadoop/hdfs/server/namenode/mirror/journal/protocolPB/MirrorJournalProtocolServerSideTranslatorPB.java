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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.JournalState;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocol;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.FinalizeLogSegmentResponseProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.GetJournalStateRequestProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.JournalResponseProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.MirrorJournalProtocolProtos.StartLogSegmentResponseProto;
import org.apache.hadoop.hdfs.server.namenode.mirror.journal.protocol.RequestInfo;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link MirrorJournalProtocolPB} to the 
 * {@link MirrorJournalProtocol} server implementation.
 */
@InterfaceAudience.Private
public class MirrorJournalProtocolServerSideTranslatorPB implements MirrorJournalProtocolPB {
  /** Server side implementation to delegate the requests to */
  private final MirrorJournalProtocol impl;

  private final static JournalResponseProto VOID_JOURNAL_RESPONSE =
  JournalResponseProto.newBuilder().build();

  private final static StartLogSegmentResponseProto
  VOID_START_LOG_SEGMENT_RESPONSE =
      StartLogSegmentResponseProto.newBuilder().build();

  public MirrorJournalProtocolServerSideTranslatorPB(MirrorJournalProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetJournalStateResponseProto getJournalState(RpcController controller,
      GetJournalStateRequestProto request) throws ServiceException {
    try {
      JournalState js = impl.getJournalState(request.getResetIPCSerial(),
          PBHelper.convert(request.getNsInfo()));
      return GetJournalStateResponseProto.newBuilder()
          .setLastCommittedTxId(js.getLastCommittedTxId())
          .build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /** @see JournalProtocol#journal */
  @Override
  public JournalResponseProto journal(RpcController unused,
      JournalRequestProto req) throws ServiceException {
    try {
      impl.journal(convert(req.getReqInfo()),
          req.getSegmentTxnId(), req.getFirstTxnId(),
          req.getNumTxns(), req.getRecords().toByteArray());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_JOURNAL_RESPONSE;
  }

  /** @see JournalProtocol#startLogSegment */
  @Override
  public StartLogSegmentResponseProto startLogSegment(RpcController controller,
      StartLogSegmentRequestProto req) throws ServiceException {
    try {
      impl.startLogSegment(convert(req.getReqInfo()),
          req.getTxid(), req.getLayoutVersion());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_START_LOG_SEGMENT_RESPONSE;
  }

  @Override
  public FinalizeLogSegmentResponseProto finalizeLogSegment(
      RpcController controller, FinalizeLogSegmentRequestProto req)
      throws ServiceException {
    try {
      impl.finalizeLogSegment(convert(req.getReqInfo()),
          req.getStartTxId(), req.getEndTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return FinalizeLogSegmentResponseProto.newBuilder().build();
  }

  private RequestInfo convert(
      MirrorJournalProtocolProtos.RequestInfoProto reqInfo) {
    return new RequestInfo(
        reqInfo.getJournalId().getIdentifier(),
        reqInfo.getIpcSerialNumber());
  }
}
