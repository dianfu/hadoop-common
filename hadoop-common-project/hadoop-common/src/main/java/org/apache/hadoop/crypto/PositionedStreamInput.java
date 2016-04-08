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
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import com.intel.chimera.input.StreamInput;

public class PositionedStreamInput extends StreamInput {
  private InputStream in;

  public PositionedStreamInput(InputStream in, int bufferSize) {
    super(in, bufferSize);
    this.in = in;
  }

  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    try {
      return ((PositionedReadable) in).read(position, buffer, offset,
          length);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "positioned read.");
    }
  }

  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    try {
      ((PositionedReadable) in).readFully(position, buffer, offset, length);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "positioned readFully.");
    }
  }

  public void seek(long pos) throws IOException {
    try {
      ((Seekable) in).seek(pos);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "seek.");
    }
  }
}
