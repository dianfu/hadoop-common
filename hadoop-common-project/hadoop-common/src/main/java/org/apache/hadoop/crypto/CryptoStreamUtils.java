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

import static com.intel.chimera.conf.ConfigurationKeys.CHIMERA_CRYPTO_STREAM_BUFFER_SIZE_KEY;
import static com.intel.chimera.conf.ConfigurationKeys.CHIMERA_CRYPTO_CIPHER_CLASSES_KEY;
import static com.intel.chimera.conf.ConfigurationKeys.CHIMERA_CRYPTO_CIPHER_JCE_PROVIDER_KEY;
import static com.intel.chimera.conf.ConfigurationKeys.CHIMERA_CRYPTO_SECURE_RANDOM_DEVICE_FILE_PATH_KEY;
import static com.intel.chimera.conf.ConfigurationKeys.CHIMERA_CRYPTO_SECURE_RANDOM_CLASSES_KEY;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_CLASSES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.random.OsSecureRandom;
import org.apache.hadoop.fs.Seekable;

import com.google.common.base.Preconditions;
import com.intel.chimera.cipher.Cipher;
import com.intel.chimera.cipher.CipherFactory;
import com.intel.chimera.cipher.CipherTransformation;
import com.intel.chimera.random.OpensslSecureRandom;

@InterfaceAudience.Private
public class CryptoStreamUtils {
  private static final int MIN_BUFFER_SIZE = 512;

  /** Forcibly free the direct buffer. */
  public static void freeDB(ByteBuffer buffer) {
    if (buffer instanceof sun.nio.ch.DirectBuffer) {
      final sun.misc.Cleaner bufferCleaner =
          ((sun.nio.ch.DirectBuffer) buffer).cleaner();
      bufferCleaner.clean();
    }
  }
  
  /** Read crypto buffer size */
  public static int getBufferSize(Configuration conf) {
    return conf.getInt(HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY, 
        HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT);
  }
  
  /** AES/CTR/NoPadding is required */
  public static void checkCodec(CryptoCodec codec) {
    if (codec.getCipherSuite() != CipherSuite.AES_CTR_NOPADDING) {
      throw new UnsupportedCodecException("AES/CTR/NoPadding is required");
    }
  }

  /** Check and floor buffer size */
  public static int checkBufferSize(CryptoCodec codec, int bufferSize) {
    Preconditions.checkArgument(bufferSize >= MIN_BUFFER_SIZE, 
        "Minimum value of buffer size is " + MIN_BUFFER_SIZE + ".");
    return bufferSize - bufferSize % codec.getCipherSuite()
        .getAlgorithmBlockSize();
  }
  
  /**
   * If input stream is {@link org.apache.hadoop.fs.Seekable}, return it's
   * current position, otherwise return 0;
   */
  public static long getInputStreamOffset(InputStream in) throws IOException {
    if (in instanceof Seekable) {
      return ((Seekable) in).getPos();
    }
    return 0;
  }

  public static Cipher getCipherInstance(Configuration conf)
      throws IOException {
    String suiteName = conf.get(HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
        HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);
    CipherSuite cipherSuite = CipherSuite.convert(suiteName);
    return getCipherInstance(conf, cipherSuite);
  }

  public static Cipher getCipherInstance(Configuration conf,
      CipherSuite cipherSuite) throws IOException {
    try {
      return CipherFactory.getInstance(
          toChimeraCipherTransformation(cipherSuite), toChimeraConf(conf));
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  private static CipherTransformation toChimeraCipherTransformation(
      CipherSuite suite) throws IOException {
    switch (suite) {
    case AES_CTR_NOPADDING:
      return CipherTransformation.AES_CTR_NOPADDING;
    default:
      throw new IOException("Suite " + suite + " is not supported.");
    }
  }

  /**
   * Get Chimera configurations from Hadoop configurations.
   */
  private static Properties toChimeraConf(Configuration conf) {
    Properties props = new Properties();

    // Set cipher classes
    String suiteName = conf.get(HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
        HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);
    String cipherString = conf.get(HADOOP_SECURITY_CRYPTO_CIPHER_CLASSES_KEY);
    if (cipherString == null) {
      CipherSuite cipherSuite = CipherSuite.convert(suiteName);
      String configName = HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX +
          cipherSuite.getConfigSuffix();
      if (configName.equals(
          HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY)) {
        cipherString = conf.get(configName,
            HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_DEFAULT);
      } else {
        cipherString = conf.get(configName);
      }
      if (cipherString != null) {
        cipherString = cipherString.replaceAll(
            OpensslAesCtrCryptoCodec.class.getCanonicalName(),
            com.intel.chimera.cipher.OpensslCipher.class.getCanonicalName());
        cipherString = cipherString.replaceAll(
            JceAesCtrCryptoCodec.class.getCanonicalName(),
            com.intel.chimera.cipher.JceCipher.class.getCanonicalName());
      }
    }
    props.setProperty(CHIMERA_CRYPTO_CIPHER_CLASSES_KEY, cipherString);

    // Set JCE provider
    String jceProvider = conf.get(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);
    if (jceProvider != null) {
      props.setProperty(CHIMERA_CRYPTO_CIPHER_JCE_PROVIDER_KEY, jceProvider);
    }

    // Set crypto buffer size
    props.setProperty(CHIMERA_CRYPTO_STREAM_BUFFER_SIZE_KEY,
        String.valueOf(getBufferSize(conf)));

    // Set random class
    String randomString = conf.get(HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY);
    if (randomString != null) {
      randomString = randomString.replaceAll(
          OsSecureRandom.class.getCanonicalName(),
          com.intel.chimera.random.OsSecureRandom.class.getCanonicalName());
      randomString = randomString.replaceAll(
          OpensslSecureRandom.class.getCanonicalName(),
          com.intel.chimera.random.OpensslSecureRandom.class
            .getCanonicalName());
      props.setProperty(CHIMERA_CRYPTO_SECURE_RANDOM_CLASSES_KEY, randomString);
    }

    // Set random dev file path
    String randomDevPath = conf.get(
        HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
        HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);
    props.setProperty(CHIMERA_CRYPTO_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, randomDevPath);

    return props;
  }
}
