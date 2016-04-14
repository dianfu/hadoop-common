/*
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
package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoOutputStream;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Provide the functionality to allow for quality-of-protection (QOP) with
 * integrity checking and privacy. It relies on CryptoInputStream and
 * CryptoOutputStream to do decryption and encryption.
 */
public class SaslCryptoCodec {
  private static final int MAC_LENGTH = 10;
  private static final int SEQ_NUM_LENGTH = 4;

  private CryptoInputStream cIn;
  private CryptoOutputStream cOut;

  private final Integrity integrity;

  public SaslCryptoCodec(Configuration conf, CipherOption cipherOption,
                         boolean isServer) throws IOException {
    CryptoCodec codec = CryptoCodec.getInstance(conf,
        cipherOption.getCipherSuite());
    byte[] inKey = cipherOption.getInKey();
    byte[] inIv = cipherOption.getInIv();
    byte[] outKey = cipherOption.getOutKey();
    byte[] outIv = cipherOption.getOutIv();
    cIn = new CryptoInputStream(null, codec,
        isServer ? inKey : outKey, isServer ? inIv : outIv);
    cOut = new CryptoOutputStream(new ByteArrayOutputStream(), codec,
        isServer ? outKey : inKey, isServer ? outIv : inIv);
    integrity = new Integrity(isServer ? outKey : inKey,
        isServer ? inKey : outKey);
  }

  public byte[] wrap(byte[] outgoing, int offset, int len)
      throws SaslException {
    // mac
    byte[] mac = integrity.getHMAC(outgoing, offset, len);
    integrity.incMySeqNum();

    // encrypt
    try {
      cOut.write(outgoing, offset, len);
      cOut.write(mac, 0, MAC_LENGTH);
      cOut.flush();
    } catch (IOException ioe) {
      throw new SaslException("Encrypt failed", ioe);
    }
    byte[] encrypted = ((ByteArrayOutputStream) cOut.getWrappedStream())
        .toByteArray();
    ((ByteArrayOutputStream) cOut.getWrappedStream()).reset();

    // append seqNum used for mac
    byte[] wrapped = new byte[encrypted.length + SEQ_NUM_LENGTH];
    System.arraycopy(encrypted, 0, wrapped, 0, encrypted.length);
    System.arraycopy(integrity.getSeqNum(), 0, wrapped,
        encrypted.length, SEQ_NUM_LENGTH);

    return wrapped;
  }

  public byte[] unwrap(byte[] incoming, int offset, int len)
      throws SaslException {
    // get seqNum
    byte[] peerSeqNum = new byte[SEQ_NUM_LENGTH];
    System.arraycopy(incoming, offset + len - SEQ_NUM_LENGTH, peerSeqNum, 0,
        SEQ_NUM_LENGTH);

    // get msg and mac
    byte[] msg = new byte[len - SEQ_NUM_LENGTH - MAC_LENGTH];
    byte[] mac = new byte[MAC_LENGTH];
    cIn.setWrappedStream(new ByteArrayInputStream(incoming, offset,
        len - SEQ_NUM_LENGTH));
    try {
      cIn.readFully(msg, 0, msg.length);
      cIn.readFully(mac, 0, mac.length);
    } catch (IOException ioe) {
      throw new SaslException("Decrypt failed" , ioe);
    }

    // check mac integrity and msg sequence
    if (!integrity.comparePeerHMAC(mac, peerSeqNum, msg, 0, msg.length)) {
      throw new SaslException("Unmatched MAC");
    }
    if (!integrity.comparePeerSeqNum(peerSeqNum)) {
      throw new SaslException("Out of order sequencing of messages. Got: "
          + integrity.byteToInt(peerSeqNum) + " Expected: "
          + integrity.peerSeqNum);
    }
    integrity.incPeerSeqNum();

    return msg;
  }

  /**
   * Helper class for providing integrity protection.
   */
  private static class Integrity {

    private int mySeqNum = 0;
    private int peerSeqNum = 0;
    private byte[] mySeqNumArray = new byte[SEQ_NUM_LENGTH];

    private byte[] myKey;
    private byte[] peerKey;

    Integrity(byte[] myKey, byte[] peerKey) throws IOException {
      this.myKey = myKey;
      this.peerKey = peerKey;
    }

    byte[] getHMAC(byte[] msg, int start, int len) throws SaslException {
      intToByte(mySeqNum);
      return calculateHMAC(myKey, mySeqNumArray, msg, start, len);
    }

    boolean comparePeerHMAC(byte[] expectedHMAC, byte[] seqNum, byte[] msg,
                            int start, int len) throws SaslException {
      byte[] mac = calculateHMAC(peerKey, seqNum, msg, start, len);
      return Arrays.equals(mac, expectedHMAC);
    }

    boolean comparePeerSeqNum(byte[] seqNum) {
      return this.peerSeqNum == byteToInt(seqNum);
    }

    byte[] getSeqNum() {
      return mySeqNumArray;
    }

    void incMySeqNum() {
      mySeqNum++;
    }

    void incPeerSeqNum() {
      peerSeqNum++;
    }

    private byte[] calculateHMAC(byte[] key, byte[] seqNum, byte[] msg,
                                 int start, int len) throws SaslException {
      byte[] seqAndMsg = new byte[SEQ_NUM_LENGTH + len];
      System.arraycopy(seqNum, 0, seqAndMsg, 0, SEQ_NUM_LENGTH);
      System.arraycopy(msg, start, seqAndMsg, SEQ_NUM_LENGTH, len);

      try {
        SecretKey keyKi = new SecretKeySpec(key, "HmacMD5");
        Mac m = Mac.getInstance("HmacMD5");
        m.init(keyKi);
        m.update(seqAndMsg);
        byte[] hMacMd5 = m.doFinal();

        /* First 10 bytes of HMAC_MD5 digest */
        byte[] macBuffer = new byte[MAC_LENGTH];
        System.arraycopy(hMacMd5, 0, macBuffer, 0, MAC_LENGTH);

        return macBuffer;
      } catch (InvalidKeyException e) {
        throw new SaslException("Invalid bytes used for key of HMAC-MD5 hash.",
            e);
      } catch (NoSuchAlgorithmException e) {
        throw new SaslException("Error creating instance of MD5 MAC algorithm",
            e);
      }
    }

    private void intToByte(int num) {
      for(int i = 3; i >= 0; i--) {
        mySeqNumArray[i] = (byte)(num & 0xff);
        num >>>= 8;
      }
    }

    private int byteToInt(byte[] seqNum) {
      int answer = 0;
      for (int i = 0; i < 4; i++) {
        answer <<= 8;
        answer |= ((int)seqNum[i] & 0xff);
      }
      return answer;
    }
  }
}
