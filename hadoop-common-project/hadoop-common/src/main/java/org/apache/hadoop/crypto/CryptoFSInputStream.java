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

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ByteBufferPool;

import com.google.common.base.Preconditions;
import com.intel.chimera.cipher.Cipher;
import com.intel.chimera.stream.PositionedCryptoInputStream;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CryptoFSInputStream extends PositionedCryptoInputStream implements 
    Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor, 
    CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess, 
    ReadableByteChannel {
  private InputStream in;

  public CryptoFSInputStream(InputStream in, Cipher cipher,
      byte[] key, byte[] iv, Configuration conf) throws IOException {
    this(in, cipher, CryptoStreamUtils.getBufferSize(conf), key, iv);
  }

  public CryptoFSInputStream(InputStream in, Cipher cipher, 
      int bufferSize, byte[] key, byte[] iv) throws IOException {
    this(in, cipher, bufferSize, key, iv, 
        CryptoStreamUtils.getInputStreamOffset(in));
  }

  public CryptoFSInputStream(InputStream in, Cipher cipher,
      int bufferSize, byte[] key, byte[] iv, long streamOffset) throws IOException {
    super(new PositionedStreamInput(in, bufferSize), cipher, bufferSize, key, iv, streamOffset);
    this.in = in;
  }

  public InputStream getWrappedStream() {
    return in;
  }

  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength,
      EnumSet<ReadOption> opts) throws IOException,
      UnsupportedOperationException {
    checkStream();
    try {
      if (outBuffer.remaining() > 0) {
        // Have some decrypted data unread, need to reset.
        ((Seekable) in).seek(getPos());
        resetStreamOffset(getPos());
      }
      final ByteBuffer buffer = ((HasEnhancedByteBufferAccess) in).
          read(bufferPool, maxLength, opts);
      if (buffer != null) {
        final int len = buffer.remaining();
        if (len > 0) {
          streamOffset += buffer.remaining(); // Read n bytes
          final int pos = buffer.position();
          decrypt(buffer, pos, len);
        }
      }
      return buffer;
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " + 
          "enhanced byte buffer access.");
    }
  }

  @Override
  public void releaseBuffer(ByteBuffer buffer) {
    try {
      ((HasEnhancedByteBufferAccess) in).releaseBuffer(buffer);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " + 
          "release buffer.");
    }
  }

  @Override
  public void setReadahead(Long readahead) throws IOException,
      UnsupportedOperationException {
    try {
      ((CanSetReadahead) in).setReadahead(readahead);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "setting the readahead caching strategy.");
    }
  }

  @Override
  public void setDropBehind(Boolean dropCache) throws IOException,
      UnsupportedOperationException {
    try {
      ((CanSetDropBehind) in).setDropBehind(dropCache);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not " +
          "support setting the drop-behind caching setting.");
    }
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    if (in instanceof HasFileDescriptor) {
      return ((HasFileDescriptor) in).getFileDescriptor();
    } else if (in instanceof FileInputStream) {
      return ((FileInputStream) in).getFD();
    } else {
      return null;
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkStream();
    return super.read(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkStream();
    super.readFully(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    checkStream();
    super.readFully(position, buffer);
  }

  @Override
  public void seek(long pos) throws IOException {
    Preconditions.checkArgument(pos >= 0, "Cannot seek to negative offset.");
    checkStream();
    super.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return super.getStreamPosition();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    Preconditions.checkArgument(targetPos >= 0, 
        "Cannot seek to negative offset.");
    checkStream();
    try {
      boolean result = ((Seekable) in).seekToNewSource(targetPos);
      resetStreamOffset(targetPos);
      return result;
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "seekToNewSource.");
    }
  }
}
