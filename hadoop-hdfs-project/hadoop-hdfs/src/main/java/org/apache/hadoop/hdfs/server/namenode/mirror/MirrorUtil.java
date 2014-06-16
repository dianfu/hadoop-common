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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;

public class MirrorUtil {

  private MirrorUtil() { /* Hidden constructor */
  }

  /**
   * @param conf Configuration
   * @param nsId nameservice id
   * 
   * @return true if the mirror cluster is enabled for nameservice {@code nsId} 
   */
  public static boolean isMirrorEnabled(final Configuration conf, String nsId) {
    Collection<String> regionIds = getRegionIds(conf, nsId);
    if (regionIds != null && regionIds.size() > 1) {
      return true;
    }
    return false;
  }

  /**
   * Return whether the current cluster is primary cluster or not after
   * making sure that mirror cluster is enabled through calling
   * {@link MirrorUtil.isMirrorEnabled}.
   */
  public static boolean isPrimaryCluster(final Configuration conf) {
    String primaryRegionId = getPrimaryRegionId(conf);
    String regionId = getRegionId(conf);
    if (primaryRegionId.equalsIgnoreCase(regionId)) {
      return true;
    }
    return false;
  }

  /**
   * Get the regionId of this region.
   */
  public static String getRegionId(final Configuration conf) {
    String regionId = conf.getTrimmed(DFSConfigKeys.DFS_REGION_ID);
    if (regionId != null && !regionId.isEmpty()) {
      return regionId;
    }
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    Collection<String> reginIds = getRegionIds(conf, nsId);
    if (1 == reginIds.size()) {
      return reginIds.toArray(new String[1])[0];
    }
    
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    String suffixes[] = DFSUtil.getSuffixIDs(conf,
        DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, namenodeId, null,
        DFSUtil.LOCAL_ADDRESS_MATCHER);
    if (suffixes == null) {
      String msg = "Configuration " + DFS_NAMENODE_RPC_ADDRESS_KEY
          + " must be suffixed with nameservice and namenode ID for HA "
          + "configuration.";
      throw new HadoopIllegalArgumentException(msg);
    }
    return suffixes[2];
  }

  /**
   * Return collection of region Ids from the configuration.
   * @param conf configuration
   * @param nsId nameservice id
   * @return collection of region Ids, or null if not specified
   */
  public static Collection<String> getRegionIds(final Configuration conf,
      String nsId) {
    return conf.getTrimmedStringCollection(DFSUtil.addKeySuffixes
        (DFSConfigKeys.DFS_REGIONS, nsId));
  }

  /**
   * Return the namenode address of primary cluster from configuration. 
   */
  public static URI getPrimaryClusterAddress(final Configuration conf) {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String primaryRegionId = getPrimaryRegionId(conf);
    return getNamenodeAddressForRegionId(conf, nsId, primaryRegionId);
  }

  /**
   * Return the region id of primary cluster from configuration. 
   */
  public static String getPrimaryRegionId(final Configuration conf) {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String primaryRegionId = conf.getTrimmed(
        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_REGION_PRIMARY, nsId));
    
    if (primaryRegionId == null || primaryRegionId.isEmpty()) {
      String msg = "Incorrect configuration: configuration "
          + DFSConfigKeys.DFS_REGION_PRIMARY + " is not configured.";
      throw new HadoopIllegalArgumentException(msg);
    }
    return primaryRegionId;
  }

  /**
   * Return the namenode URI for specified nameservice {@code nsId} in
   * specified region {@code regionId}. 
   */
  static URI getNamenodeAddressForRegionId(final Configuration conf,
      String nsId, String regionId) {
    URI ret = null;
    if (HAUtil.isHAEnabled(conf, nsId, regionId)) {
      // Create the logical URI of the nameservice.
      try {
        ret = new URI(HdfsConstants.HDFS_URI_SCHEME + "://" + nsId);
      } catch (URISyntaxException ue) {
        throw new IllegalArgumentException(ue);
      }
    } else {
      String addr = conf.get(DFSUtil.concatSuffixes(
          DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, regionId));
      if (addr != null) {
        ret = DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
            NetUtils.createSocketAddr(addr, NameNode.DEFAULT_PORT));
      }
    }

    if (ret == null) {
      String errMsg = "Cannot find configuration for region "
          + regionId + " in configuration " 
          + DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY
          + (nsId == null || nsId.isEmpty() ? "" : "." + nsId);
      throw new HadoopIllegalArgumentException(errMsg);
    } else {
      return ret;
    }
  }

  public static URI getMirrorLoggerAddress(
      final Configuration conf, String regionId) {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    return getNamenodeAddressForRegionId(conf, nsId, regionId);
  }

  /**
   * Returns list of URI corresponding to NN RPC addresses of mirror
   * clusters from configuration.
   * 
   * @param conf configration
   * @return map{region -> URI}
   */
  public static Map<String, URI> getMirrorAddressList(final Configuration conf) {
    Map<String, URI> addrMap = new HashMap<String, URI>();
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String primaryRegionId = getPrimaryRegionId(conf);
    Collection<String> reginIds = getRegionIds(conf, nsId);
    for (String regionId : reginIds) {
      if (!primaryRegionId.equalsIgnoreCase(regionId)) {
        addrMap.put(regionId,
            getNamenodeAddressForRegionId(conf, nsId, regionId));
      }
    }

    return addrMap;
  }
}
