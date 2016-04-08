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
package org.apache.hadoop.protocolPB;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.security.proto.SecurityProtos.CipherSuiteProto;
import org.apache.hadoop.security.proto.SecurityProtos.CipherOptionProto;

import java.util.List;

public class CommonPBHelper {

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  public static CipherSuiteProto convert(CipherSuite suite) {
    switch (suite) {
      case UNKNOWN:
        return CipherSuiteProto.UNKNOWN;
      case AES_CTR_NOPADDING:
        return CipherSuiteProto.AES_CTR_NOPADDING;
      default:
        return null;
    }
  }

  public static CipherSuite convert(CipherSuiteProto proto) {
    switch (proto) {
      case AES_CTR_NOPADDING:
        return CipherSuite.AES_CTR_NOPADDING;
      default:
        // Set to UNKNOWN and stash the unknown enum value
        CipherSuite suite = CipherSuite.UNKNOWN;
        suite.setUnknownValue(proto.getNumber());
        return suite;
    }
  }

  public static CipherOptionProto convert(CipherOption option) {
    if (option != null) {
      CipherOptionProto.Builder builder =
          CipherOptionProto.newBuilder();
      if (option.getCipherSuite() != null) {
        builder.setSuite(convert(option.getCipherSuite()));
      }
      if (option.getInKey() != null) {
        builder.setInKey(getByteString(option.getInKey()));
      }
      if (option.getInIv() != null) {
        builder.setInIv(getByteString(option.getInIv()));
      }
      if (option.getOutKey() != null) {
        builder.setOutKey(getByteString(option.getOutKey()));
      }
      if (option.getOutIv() != null) {
        builder.setOutIv(getByteString(option.getOutIv()));
      }
      return builder.build();
    }
    return null;
  }

  public static CipherOption convert(CipherOptionProto proto) {
    if (proto != null) {
      CipherSuite suite = null;
      if (proto.getSuite() != null) {
        suite = convert(proto.getSuite());
      }
      byte[] inKey = null;
      if (proto.getInKey() != null) {
        inKey = proto.getInKey().toByteArray();
      }
      byte[] inIv = null;
      if (proto.getInIv() != null) {
        inIv = proto.getInIv().toByteArray();
      }
      byte[] outKey = null;
      if (proto.getOutKey() != null) {
        outKey = proto.getOutKey().toByteArray();
      }
      byte[] outIv = null;
      if (proto.getOutIv() != null) {
        outIv = proto.getOutIv().toByteArray();
      }
      return new CipherOption(suite, inKey, inIv, outKey, outIv);
    }
    return null;
  }

  public static List<CipherOptionProto> convertCipherOptions(
      List<CipherOption> options) {
    if (options != null) {
      List<CipherOptionProto> protos =
          Lists.newArrayListWithCapacity(options.size());
      for (CipherOption option : options) {
        protos.add(convert(option));
      }
      return protos;
    }
    return null;
  }

  public static List<CipherOption> convertCipherOptionProtos(
      List<CipherOptionProto> protos) {
    if (protos != null) {
      List<CipherOption> options =
          Lists.newArrayListWithCapacity(protos.size());
      for (CipherOptionProto proto : protos) {
        options.add(convert(proto));
      }
      return options;
    }
    return null;
  }
}
