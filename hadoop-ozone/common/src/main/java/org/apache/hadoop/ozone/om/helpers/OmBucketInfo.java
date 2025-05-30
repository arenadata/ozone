/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;

/**
 * A class that encapsulates Bucket Info.
 */
public final class OmBucketInfo extends WithObjectID implements Auditable, CopyObject<OmBucketInfo> {
  private static final Codec<OmBucketInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(BucketInfo.getDefaultInstance()),
      OmBucketInfo::getFromProtobuf,
      OmBucketInfo::getProtobuf,
      OmBucketInfo.class);

  /**
   * Name of the volume in which the bucket belongs to.
   */
  private final String volumeName;
  /**
   * Name of the bucket.
   */
  private final String bucketName;
  /**
   * ACL Information (mutable).
   */
  private final CopyOnWriteArrayList<OzoneAcl> acls;
  /**
   * Bucket Version flag.
   */
  private final boolean isVersionEnabled;
  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private final StorageType storageType;
  /**
   * Creation time of bucket.
   */
  private final long creationTime;
  /**
   * modification time of bucket.
   */
  private long modificationTime;

  /**
   * Bucket encryption key info if encryption is enabled.
   */
  private final BucketEncryptionKeyInfo bekInfo;

  /**
   * Optional default replication for bucket.
   */
  private final DefaultReplicationConfig defaultReplicationConfig;

  private final String sourceVolume;

  private final String sourceBucket;

  private long usedBytes;
  private long usedNamespace;
  private final long quotaInBytes;
  private final long quotaInNamespace;

  /**
   * Bucket Layout.
   */
  private final BucketLayout bucketLayout;

  private String owner;

  private OmBucketInfo(Builder b) {
    super(b);
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.acls = new CopyOnWriteArrayList<>(b.acls);
    this.isVersionEnabled = b.isVersionEnabled;
    this.storageType = b.storageType;
    this.creationTime = b.creationTime;
    this.modificationTime = b.modificationTime;
    this.bekInfo = b.bekInfo;
    this.sourceVolume = b.sourceVolume;
    this.sourceBucket = b.sourceBucket;
    this.usedBytes = b.usedBytes;
    this.usedNamespace = b.usedNamespace;
    this.quotaInBytes = b.quotaInBytes;
    this.quotaInNamespace = b.quotaInNamespace;
    this.bucketLayout = b.bucketLayout;
    this.owner = b.owner;
    this.defaultReplicationConfig = b.defaultReplicationConfig;
  }

  public static Codec<OmBucketInfo> getCodec() {
    return CODEC;
  }

  /**
   * Returns the Volume Name.
   * @return String.
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns the Bucket Name.
   * @return String
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Returns the ACL's associated with this bucket.
   * @return {@literal List<OzoneAcl>}
   */
  public List<OzoneAcl> getAcls() {
    return ImmutableList.copyOf(acls);
  }

  /**
   * Add an ozoneAcl to list of existing Acl set.
   * @param ozoneAcl
   * @return true - if successfully added, false if not added or acl is
   * already existing in the acl list.
   */
  public boolean addAcl(OzoneAcl ozoneAcl) {
    return OzoneAclUtil.addAcl(acls, ozoneAcl);
  }

  /**
   * Remove acl from existing acl list.
   * @param ozoneAcl
   * @return true - if successfully removed, false if not able to remove due
   * to that acl is not in the existing acl list.
   */
  public boolean removeAcl(OzoneAcl ozoneAcl) {
    return OzoneAclUtil.removeAcl(acls, ozoneAcl);
  }

  /**
   * Reset the existing acl list.
   * @param ozoneAcls
   * @return true - if successfully able to reset.
   */
  public boolean setAcls(List<OzoneAcl> ozoneAcls) {
    return OzoneAclUtil.setAcl(acls, ozoneAcls);
  }

  /**
   * Returns true if bucket version is enabled, else false.
   * @return isVersionEnabled
   */
  public boolean getIsVersionEnabled() {
    return isVersionEnabled;
  }

  /**
   * Returns the type of storage to be used.
   * @return StorageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns creation time.
   *
   * @return long
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns modification time.
   * @return long
   */
  public long getModificationTime() {
    return modificationTime;
  }

  /**
   * Returns bucket encryption key info.
   * @return bucket encryption key info
   */
  public BucketEncryptionKeyInfo getEncryptionKeyInfo() {
    return bekInfo;
  }

  /**
   * Returns the Bucket Layout.
   *
   * @return BucketLayout.
   */
  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  /**
   * Returns bucket EC replication config.
   *
   * @return EC replication config.
   */
  public DefaultReplicationConfig getDefaultReplicationConfig() {
    return defaultReplicationConfig;
  }

  public String getSourceVolume() {
    return sourceVolume;
  }

  public String getSourceBucket() {
    return sourceBucket;
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public long getUsedNamespace() {
    return usedNamespace;
  }

  public void incrUsedBytes(long bytes) {
    this.usedBytes += bytes;
  }

  public void incrUsedNamespace(long namespaceToUse) {
    this.usedNamespace += namespaceToUse;
  }

  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  public boolean isLink() {
    return sourceVolume != null && sourceBucket != null;
  }

  public String getOwner() {
    return owner;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public void setOwner(String ownerName) {
    this.owner = ownerName;
  }

  /**
   * Returns new builder class that builds a OmBucketInfo.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volumeName);
    auditMap.put(OzoneConsts.BUCKET, this.bucketName);
    auditMap.put(OzoneConsts.BUCKET_LAYOUT, String.valueOf(this.bucketLayout));
    auditMap.put(OzoneConsts.GDPR_FLAG,
        getMetadata().get(OzoneConsts.GDPR_FLAG));
    auditMap.put(OzoneConsts.ACLS,
        (this.acls != null) ? this.acls.toString() : null);
    auditMap.put(OzoneConsts.IS_VERSION_ENABLED,
        String.valueOf(this.isVersionEnabled));
    auditMap.put(OzoneConsts.STORAGE_TYPE,
        (this.storageType != null) ? this.storageType.name() : null);
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));
    auditMap.put(OzoneConsts.BUCKET_ENCRYPTION_KEY,
        (bekInfo != null) ? bekInfo.getKeyName() : null);
    auditMap.put(OzoneConsts.MODIFICATION_TIME,
        String.valueOf(this.modificationTime));
    if (isLink()) {
      auditMap.put(OzoneConsts.SOURCE_VOLUME, sourceVolume);
      auditMap.put(OzoneConsts.SOURCE_BUCKET, sourceBucket);
    }
    auditMap.put(OzoneConsts.USED_BYTES, String.valueOf(this.usedBytes));
    auditMap.put(OzoneConsts.USED_NAMESPACE,
        String.valueOf(this.usedNamespace));
    auditMap.put(OzoneConsts.OWNER, this.owner);
    auditMap.put(OzoneConsts.REPLICATION_TYPE,
        (this.defaultReplicationConfig != null) ?
            String.valueOf(this.defaultReplicationConfig.getType()) : null);
    auditMap.put(OzoneConsts.REPLICATION_CONFIG,
        (this.defaultReplicationConfig != null) ?
            this.defaultReplicationConfig.getReplicationConfig()
                .getReplication() : null);
    auditMap.put(OzoneConsts.QUOTA_IN_BYTES, String.valueOf(this.quotaInBytes));
    auditMap.put(OzoneConsts.QUOTA_IN_NAMESPACE,
        String.valueOf(this.quotaInNamespace));
    return auditMap;
  }

  @Override
  public OmBucketInfo copyObject() {
    Builder builder = toBuilder();

    if (bekInfo != null) {
      builder.setBucketEncryptionKey(bekInfo.copy());
    }

    if (defaultReplicationConfig != null) {
      builder.setDefaultReplicationConfig(defaultReplicationConfig.copy());
    }

    return builder.build();
  }

  public Builder toBuilder() {
    return new Builder(this)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStorageType(storageType)
        .setIsVersionEnabled(isVersionEnabled)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setBucketEncryptionKey(bekInfo)
        .setSourceVolume(sourceVolume)
        .setSourceBucket(sourceBucket)
        .setAcls(acls)
        .setUsedBytes(usedBytes)
        .setUsedNamespace(usedNamespace)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace)
        .setBucketLayout(bucketLayout)
        .setOwner(owner)
        .setDefaultReplicationConfig(defaultReplicationConfig);
  }

  /**
   * Builder for OmBucketInfo.
   */
  public static class Builder extends WithObjectID.Builder {
    private String volumeName;
    private String bucketName;
    private final List<OzoneAcl> acls = new ArrayList<>();
    private boolean isVersionEnabled;
    private StorageType storageType = StorageType.DISK;
    private long creationTime;
    private long modificationTime;
    private BucketEncryptionKeyInfo bekInfo;
    private String sourceVolume;
    private String sourceBucket;
    private long usedBytes;
    private long usedNamespace;
    private long quotaInBytes = OzoneConsts.QUOTA_RESET;
    private long quotaInNamespace = OzoneConsts.QUOTA_RESET;
    private BucketLayout bucketLayout = BucketLayout.DEFAULT;
    private String owner;
    private DefaultReplicationConfig defaultReplicationConfig;

    public Builder() {
    }

    private Builder(OmBucketInfo obj) {
      super(obj);
    }

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      if (listOfAcls != null) {
        this.acls.addAll(listOfAcls);
      }
      return this;
    }

    public List<OzoneAcl> getAcls() {
      return acls;
    }

    public Builder addAcl(OzoneAcl ozoneAcl) {
      if (ozoneAcl != null) {
        this.acls.add(ozoneAcl);
      }
      return this;
    }

    public Builder setIsVersionEnabled(boolean versionFlag) {
      this.isVersionEnabled = versionFlag;
      return this;
    }

    public Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public Builder setCreationTime(long createdOn) {
      this.creationTime = createdOn;
      return this;
    }

    public Builder setModificationTime(long modifiedOn) {
      this.modificationTime = modifiedOn;
      return this;
    }

    @Override
    public Builder setObjectID(long obId) {
      super.setObjectID(obId);
      return this;
    }

    @Override
    public Builder setUpdateID(long id) {
      super.setUpdateID(id);
      return this;
    }

    @Override
    public Builder addMetadata(String key, String value) {
      super.addMetadata(key, value);
      return this;
    }

    @Override
    public Builder addAllMetadata(Map<String, String> additionalMetadata) {
      super.addAllMetadata(additionalMetadata);
      return this;
    }

    public Builder setBucketEncryptionKey(
        BucketEncryptionKeyInfo info) {
      this.bekInfo = info;
      return this;
    }

    /** @param volume - source volume for bucket links, null otherwise */
    public Builder setSourceVolume(String volume) {
      this.sourceVolume = volume;
      return this;
    }

    /** @param bucket - source bucket for bucket links, null otherwise */
    public Builder setSourceBucket(String bucket) {
      this.sourceBucket = bucket;
      return this;
    }

    /** @param quotaUsage - Bucket Quota Usage in bytes. */
    public Builder setUsedBytes(long quotaUsage) {
      this.usedBytes = quotaUsage;
      return this;
    }

    /** @param quotaUsage - Bucket Quota Usage in counts. */
    public Builder setUsedNamespace(long quotaUsage) {
      this.usedNamespace = quotaUsage;
      return this;
    }

    /** @param quota Bucket quota in bytes. */
    public Builder setQuotaInBytes(long quota) {
      this.quotaInBytes = quota;
      return this;
    }

    /** @param quota Bucket quota in counts. */
    public Builder setQuotaInNamespace(long quota) {
      this.quotaInNamespace = quota;
      return this;
    }

    public Builder setBucketLayout(BucketLayout type) {
      this.bucketLayout = type;
      return this;
    }

    public Builder setOwner(String ownerName) {
      this.owner = ownerName;
      return this;
    }

    public Builder setDefaultReplicationConfig(
        DefaultReplicationConfig defaultReplConfig) {
      this.defaultReplicationConfig = defaultReplConfig;
      return this;
    }

    /**
     * Constructs the OmBucketInfo.
     * @return instance of OmBucketInfo.
     */
    public OmBucketInfo build() {
      Preconditions.checkNotNull(volumeName);
      Preconditions.checkNotNull(bucketName);
      Preconditions.checkNotNull(acls);
      Preconditions.checkNotNull(storageType);
      return new OmBucketInfo(this);
    }
  }

  /**
   * Creates BucketInfo protobuf from OmBucketInfo.
   */
  public BucketInfo getProtobuf() {
    BucketInfo.Builder bib =  BucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllAcls(OzoneAclUtil.toProtobuf(acls))
        .setIsVersionEnabled(isVersionEnabled)
        .setStorageType(storageType.toProto())
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(getObjectID())
        .setUpdateID(getUpdateID())
        .setUsedBytes(usedBytes)
        .setUsedNamespace(usedNamespace)
        .addAllMetadata(KeyValueUtil.toProtobuf(getMetadata()))
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace);
    if (bucketLayout != null) {
      bib.setBucketLayout(bucketLayout.toProto());
    }
    if (bekInfo != null && bekInfo.getKeyName() != null) {
      bib.setBeinfo(OMPBHelper.convert(bekInfo));
    }
    if (defaultReplicationConfig != null) {
      bib.setDefaultReplicationConfig(defaultReplicationConfig.toProto());
    }
    if (sourceVolume != null) {
      bib.setSourceVolume(sourceVolume);
    }
    if (sourceBucket != null) {
      bib.setSourceBucket(sourceBucket);
    }
    if (owner != null) {
      bib.setOwner(owner);
    }
    return bib.build();
  }

  /**
   * Parses BucketInfo protobuf and creates OmBucketInfo.
   * @param bucketInfo
   * @return instance of OmBucketInfo
   */
  public static OmBucketInfo getFromProtobuf(BucketInfo bucketInfo) {
    return getFromProtobuf(bucketInfo, null);
  }

  /**
   * Parses BucketInfo protobuf and creates OmBucketInfo.
   * @param bucketInfo
   * @return instance of OmBucketInfo
   */
  public static OmBucketInfo getFromProtobuf(BucketInfo bucketInfo,
      BucketLayout buckLayout) {
    Builder obib = OmBucketInfo.newBuilder()
        .setVolumeName(bucketInfo.getVolumeName())
        .setBucketName(bucketInfo.getBucketName())
        .setAcls(bucketInfo.getAclsList().stream().map(
            OzoneAcl::fromProtobuf).collect(Collectors.toList()))
        .setIsVersionEnabled(bucketInfo.getIsVersionEnabled())
        .setStorageType(StorageType.valueOf(bucketInfo.getStorageType()))
        .setCreationTime(bucketInfo.getCreationTime())
        .setUsedBytes(bucketInfo.getUsedBytes())
        .setModificationTime(bucketInfo.getModificationTime())
        .setQuotaInBytes(bucketInfo.getQuotaInBytes())
        .setUsedNamespace(bucketInfo.getUsedNamespace())
        .setQuotaInNamespace(bucketInfo.getQuotaInNamespace());
    if (buckLayout != null) {
      obib.setBucketLayout(buckLayout);
    } else if (bucketInfo.getBucketLayout() != null) {
      obib.setBucketLayout(
          BucketLayout.fromProto(bucketInfo.getBucketLayout()));
    }
    if (bucketInfo.hasDefaultReplicationConfig()) {
      obib.setDefaultReplicationConfig(
          DefaultReplicationConfig.fromProto(
              bucketInfo.getDefaultReplicationConfig()));
    }
    if (bucketInfo.hasObjectID()) {
      obib.setObjectID(bucketInfo.getObjectID());
    }
    if (bucketInfo.hasUpdateID()) {
      obib.setUpdateID(bucketInfo.getUpdateID());
    }
    if (bucketInfo.getMetadataList() != null) {
      obib.addAllMetadata(KeyValueUtil
          .getFromProtobuf(bucketInfo.getMetadataList()));
    }
    if (bucketInfo.hasBeinfo()) {
      obib.setBucketEncryptionKey(OMPBHelper.convert(bucketInfo.getBeinfo()));
    }
    if (bucketInfo.hasSourceVolume()) {
      obib.setSourceVolume(bucketInfo.getSourceVolume());
    }
    if (bucketInfo.hasSourceBucket()) {
      obib.setSourceBucket(bucketInfo.getSourceBucket());
    }
    if (bucketInfo.hasOwner()) {
      obib.setOwner(bucketInfo.getOwner());
    }
    return obib.build();
  }

  @Override
  public String getObjectInfo() {
    String sourceInfo = sourceVolume != null && sourceBucket != null
        ? ", source='" + sourceVolume + "/" + sourceBucket + "'"
        : "";

    return "OMBucketInfo{" +
        "volume='" + volumeName + "'" +
        ", bucket='" + bucketName + "'" +
        ", isVersionEnabled='" + isVersionEnabled + "'" +
        ", storageType='" + storageType + "'" +
        ", creationTime='" + creationTime + "'" +
        ", usedBytes='" + usedBytes + "'" +
        ", usedNamespace='" + usedNamespace + "'" +
        ", quotaInBytes='" + quotaInBytes + "'" +
        ", quotaInNamespace='" + quotaInNamespace + "'" +
        ", bucketLayout='" + bucketLayout + '\'' +
        ", defaultReplicationConfig='" + defaultReplicationConfig + '\'' +
        sourceInfo +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmBucketInfo that = (OmBucketInfo) o;
    return creationTime == that.creationTime &&
        modificationTime == that.modificationTime &&
        volumeName.equals(that.volumeName) &&
        bucketName.equals(that.bucketName) &&
        Objects.equals(acls, that.acls) &&
        Objects.equals(isVersionEnabled, that.isVersionEnabled) &&
        storageType == that.storageType &&
        getObjectID() == that.getObjectID() &&
        getUpdateID() == that.getUpdateID() &&
        usedBytes == that.usedBytes &&
        usedNamespace == that.usedNamespace &&
        Objects.equals(sourceVolume, that.sourceVolume) &&
        Objects.equals(sourceBucket, that.sourceBucket) &&
        Objects.equals(getMetadata(), that.getMetadata()) &&
        Objects.equals(bekInfo, that.bekInfo) &&
        Objects.equals(owner, that.owner) &&
        Objects.equals(defaultReplicationConfig, that.defaultReplicationConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName);
  }

  @Override
  public String toString() {
    return "OmBucketInfo{" +
        "volumeName='" + volumeName + "'" +
        ", bucketName='" + bucketName + "'" +
        ", acls=" + acls +
        ", isVersionEnabled=" + isVersionEnabled +
        ", storageType=" + storageType +
        ", creationTime=" + creationTime +
        ", bekInfo=" + bekInfo +
        ", sourceVolume='" + sourceVolume + "'" +
        ", sourceBucket='" + sourceBucket + "'" +
        ", objectID=" + getObjectID() +
        ", updateID=" + getUpdateID() +
        ", metadata=" + getMetadata() +
        ", usedBytes=" + usedBytes +
        ", usedNamespace=" + usedNamespace +
        ", quotaInBytes=" + quotaInBytes +
        ", quotaInNamespace=" + quotaInNamespace +
        ", bucketLayout=" + bucketLayout +
        ", owner=" + owner +
        ", defaultReplicationConfig=" + defaultReplicationConfig +
        '}';
  }
}
