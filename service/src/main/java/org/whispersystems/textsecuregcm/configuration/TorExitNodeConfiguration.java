/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.constraints.NotBlank;
import java.time.Duration;

public class TorExitNodeConfiguration {

  @JsonProperty
  @NotBlank
  private String s3Region;

  @JsonProperty
  @NotBlank
  private String s3Bucket;

  @JsonProperty
  @NotBlank
  private String objectKey;

  @JsonProperty
  private Duration refreshInterval = Duration.ofMinutes(5);

  public String getS3Region() {
    return s3Region;
  }

  @VisibleForTesting
  public void setS3Region(final String s3Region) {
    this.s3Region = s3Region;
  }

  public String getS3Bucket() {
    return s3Bucket;
  }

  public String getObjectKey() {
    return objectKey;
  }

  public Duration getRefreshInterval() {
    return refreshInterval;
  }
}
