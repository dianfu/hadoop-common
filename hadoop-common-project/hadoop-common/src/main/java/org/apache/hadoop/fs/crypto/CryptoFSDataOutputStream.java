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
package org.apache.hadoop.fs.crypto;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoFSOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.intel.chimera.cipher.Cipher;

public class CryptoFSDataOutputStream extends FSDataOutputStream {
  private final FSDataOutputStream fsOut;
  
  public CryptoFSDataOutputStream(FSDataOutputStream out, Cipher cipher,
      int bufferSize, byte[] key, byte[] iv) throws IOException {
    super(new CryptoFSOutputStream(out, cipher, bufferSize, key, iv, 
        out.getPos()), null, out.getPos()); 
    this.fsOut = out;
  }
  
  public CryptoFSDataOutputStream(FSDataOutputStream out, Cipher cipher,
      byte[] key, byte[] iv, Configuration conf) throws IOException {
    super(new CryptoFSOutputStream(out, cipher, key, iv, out.getPos(), conf), 
        null, out.getPos()); 
    this.fsOut = out;
  }
  
  @Override
  public long getPos() {
    return fsOut.getPos();
  }
}
