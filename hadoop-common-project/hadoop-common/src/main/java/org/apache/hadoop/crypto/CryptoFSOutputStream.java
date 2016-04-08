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
package org.apache.hadoop.crypto;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.Syncable;

import com.intel.chimera.cipher.Cipher;
import com.intel.chimera.stream.PositionedCryptoOutputStream;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CryptoFSOutputStream extends PositionedCryptoOutputStream implements
    Syncable, CanSetDropBehind {
  private OutputStream out;

  public CryptoFSOutputStream(OutputStream out, Cipher cipher,
      int bufferSize, byte[] key, byte[] iv) throws IOException {
    this(out, cipher, bufferSize, key, iv, 0);
  }

  public CryptoFSOutputStream(OutputStream out, Cipher cipher,
      byte[] key, byte[] iv, Configuration conf) throws IOException {
    this(out, cipher, key, iv, 0, conf);
  }

  public CryptoFSOutputStream(OutputStream out, Cipher cipher,
      byte[] key, byte[] iv, long streamOffset, Configuration conf) throws IOException {
    this(out, cipher, CryptoStreamUtils.getBufferSize(conf),
        key, iv, streamOffset);
  }

  public CryptoFSOutputStream(OutputStream out, Cipher cipher,
      int bufferSize, byte[] key, byte[] iv, long streamOffset)
      throws IOException {
    super(out, cipher, bufferSize, key, iv, streamOffset);
    this.out = out;
  }

  public OutputStream getWrappedStream() {
    return out;
  }

  @Override
  public void setDropBehind(Boolean dropCache) throws IOException,
      UnsupportedOperationException {
    try {
      ((CanSetDropBehind) out).setDropBehind(dropCache);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not " +
          "support setting the drop-behind caching.");
    }
  }

  @Override
  public void hflush() throws IOException {
    flush();
    if (out instanceof Syncable) {
      ((Syncable)out).hflush();
    }
  }

  @Override
  public void hsync() throws IOException {
    flush();
    if (out instanceof Syncable) {
      ((Syncable)out).hsync();
    }
  }
}
