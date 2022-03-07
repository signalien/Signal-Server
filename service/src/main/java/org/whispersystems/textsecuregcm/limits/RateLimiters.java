/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;


import java.util.concurrent.atomic.AtomicReference;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.CardinalityRateLimitConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.RateLimitConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import javax.annotation.Nullable;

public class RateLimiters {

  private final RateLimiter smsDestinationLimiter;
  private final RateLimiter voiceDestinationLimiter;
  private final RateLimiter voiceDestinationDailyLimiter;
  private final RateLimiter smsVoiceIpLimiter;
  private final RateLimiter smsVoicePrefixLimiter;
  private final RateLimiter autoBlockLimiter;
  private final RateLimiter verifyLimiter;
  private final RateLimiter pinLimiter;

  private final RateLimiter attachmentLimiter;
  private final RateLimiter contactsLimiter;
  private final RateLimiter contactsIpLimiter;
  private final RateLimiter preKeysLimiter;
  private final RateLimiter messagesLimiter;

  private final RateLimiter allocateDeviceLimiter;
  private final RateLimiter verifyDeviceLimiter;

  private final RateLimiter turnLimiter;

  private final RateLimiter profileLimiter;
  private final RateLimiter stickerPackLimiter;
  private final RateLimiter usernameLookupLimiter;
  private final RateLimiter usernameSetLimiter;

  private final AtomicReference<CardinalityRateLimiter> unsealedSenderLimiter;
  private final AtomicReference<RateLimiter> unsealedIpLimiter;

  private final FaultTolerantRedisCluster   cacheCluster;
  private final FaultTolerantRedisCluster   newCacheCluster;
  private final DynamicConfigurationManager dynamicConfig;

  public RateLimiters(RateLimitsConfiguration config, DynamicConfigurationManager dynamicConfig, FaultTolerantRedisCluster cacheCluster, FaultTolerantRedisCluster newCacheCluster) {
    this.cacheCluster    = cacheCluster;
    this.newCacheCluster = newCacheCluster;
    this.dynamicConfig   = dynamicConfig;

    this.smsDestinationLimiter = new RateLimiter(cacheCluster, "smsDestination",
                                                 config.getSmsDestination().getBucketSize(),
                                                 config.getSmsDestination().getLeakRatePerMinute());

    this.voiceDestinationLimiter = new RateLimiter(cacheCluster, "voxDestination",
                                                   config.getVoiceDestination().getBucketSize(),
                                                   config.getVoiceDestination().getLeakRatePerMinute());

    this.voiceDestinationDailyLimiter = new RateLimiter(cacheCluster, "voxDestinationDaily",
                                                        config.getVoiceDestinationDaily().getBucketSize(),
                                                        config.getVoiceDestinationDaily().getLeakRatePerMinute());

    this.smsVoiceIpLimiter = new RateLimiter(cacheCluster, "smsVoiceIp",
                                             config.getSmsVoiceIp().getBucketSize(),
                                             config.getSmsVoiceIp().getLeakRatePerMinute());

    this.smsVoicePrefixLimiter = new RateLimiter(cacheCluster, "smsVoicePrefix",
                                                 config.getSmsVoicePrefix().getBucketSize(),
                                                 config.getSmsVoicePrefix().getLeakRatePerMinute());

    this.autoBlockLimiter = new RateLimiter(cacheCluster, newCacheCluster, "autoBlock",
                                            config.getAutoBlock().getBucketSize(),
                                            config.getAutoBlock().getLeakRatePerMinute());

    this.verifyLimiter = new LockingRateLimiter(cacheCluster, newCacheCluster, "verify",
                                                config.getVerifyNumber().getBucketSize(),
                                                config.getVerifyNumber().getLeakRatePerMinute());

    this.pinLimiter = new LockingRateLimiter(cacheCluster, "pin",
                                             config.getVerifyPin().getBucketSize(),
                                             config.getVerifyPin().getLeakRatePerMinute());

    this.attachmentLimiter = new RateLimiter(cacheCluster, "attachmentCreate",
                                             config.getAttachments().getBucketSize(),
                                             config.getAttachments().getLeakRatePerMinute());

    this.contactsLimiter = new RateLimiter(cacheCluster, "contactsQuery",
                                           config.getContactQueries().getBucketSize(),
                                           config.getContactQueries().getLeakRatePerMinute());

    this.contactsIpLimiter = new RateLimiter(cacheCluster, "contactsIpQuery",
                                             config.getContactIpQueries().getBucketSize(),
                                             config.getContactIpQueries().getLeakRatePerMinute());

    this.preKeysLimiter = new RateLimiter(cacheCluster, "prekeys",
                                          config.getPreKeys().getBucketSize(),
                                          config.getPreKeys().getLeakRatePerMinute());

    this.messagesLimiter = new RateLimiter(cacheCluster, "messages",
                                           config.getMessages().getBucketSize(),
                                           config.getMessages().getLeakRatePerMinute());

    this.allocateDeviceLimiter = new RateLimiter(cacheCluster, "allocateDevice",
                                                 config.getAllocateDevice().getBucketSize(),
                                                 config.getAllocateDevice().getLeakRatePerMinute());

    this.verifyDeviceLimiter = new RateLimiter(cacheCluster, "verifyDevice",
                                               config.getVerifyDevice().getBucketSize(),
                                               config.getVerifyDevice().getLeakRatePerMinute());

    this.turnLimiter = new RateLimiter(cacheCluster, "turnAllocate",
                                       config.getTurnAllocations().getBucketSize(),
                                       config.getTurnAllocations().getLeakRatePerMinute());

    this.profileLimiter = new RateLimiter(cacheCluster, newCacheCluster, "profile",
                                          config.getProfile().getBucketSize(),
                                          config.getProfile().getLeakRatePerMinute());

    this.stickerPackLimiter = new RateLimiter(cacheCluster, "stickerPack",
                                              config.getStickerPack().getBucketSize(),
                                              config.getStickerPack().getLeakRatePerMinute());

    this.usernameLookupLimiter = new RateLimiter(cacheCluster, "usernameLookup",
                                                 config.getUsernameLookup().getBucketSize(),
                                                 config.getUsernameLookup().getLeakRatePerMinute());

    this.usernameSetLimiter = new RateLimiter(cacheCluster, "usernameSet",
                                              config.getUsernameSet().getBucketSize(),
                                              config.getUsernameSet().getLeakRatePerMinute());

    this.unsealedSenderLimiter = new AtomicReference<>(createUnsealedSenderLimiter(cacheCluster, null, dynamicConfig.getConfiguration().getLimits().getUnsealedSenderNumber()));
    this.unsealedIpLimiter     = new AtomicReference<>(createUnsealedIpLimiter(cacheCluster, newCacheCluster, dynamicConfig.getConfiguration().getLimits().getUnsealedSenderIp()));
  }

  public CardinalityRateLimiter getUnsealedSenderLimiter() {
    CardinalityRateLimitConfiguration currentConfiguration = dynamicConfig.getConfiguration().getLimits().getUnsealedSenderNumber();

    return this.unsealedSenderLimiter.updateAndGet(rateLimiter -> {
      if (rateLimiter.hasConfiguration(currentConfiguration)) {
        return rateLimiter;
      } else {
        return createUnsealedSenderLimiter(cacheCluster, null, currentConfiguration);
      }
    });
  }

  public RateLimiter getUnsealedIpLimiter() {
    RateLimitConfiguration currentConfiguration = dynamicConfig.getConfiguration().getLimits().getUnsealedSenderIp();

    return this.unsealedIpLimiter.updateAndGet(rateLimiter -> {
      if (rateLimiter.hasConfiguration(currentConfiguration)) {
        return rateLimiter;
      } else {
        return createUnsealedIpLimiter(cacheCluster, newCacheCluster, currentConfiguration);
      }
    });
  }

  public RateLimiter getAllocateDeviceLimiter() {
    return allocateDeviceLimiter;
  }

  public RateLimiter getVerifyDeviceLimiter() {
    return verifyDeviceLimiter;
  }

  public RateLimiter getMessagesLimiter() {
    return messagesLimiter;
  }

  public RateLimiter getPreKeysLimiter() {
    return preKeysLimiter;
  }

  public RateLimiter getContactsLimiter() {
    return contactsLimiter;
  }

  public RateLimiter getContactsIpLimiter() {
    return contactsIpLimiter;
  }

  public RateLimiter getAttachmentLimiter() {
    return this.attachmentLimiter;
  }

  public RateLimiter getSmsDestinationLimiter() {
    return smsDestinationLimiter;
  }

  public RateLimiter getSmsVoiceIpLimiter() {
    return smsVoiceIpLimiter;
  }

  public RateLimiter getSmsVoicePrefixLimiter() {
    return smsVoicePrefixLimiter;
  }

  public RateLimiter getAutoBlockLimiter() {
    return autoBlockLimiter;
  }

  public RateLimiter getVoiceDestinationLimiter() {
    return voiceDestinationLimiter;
  }

  public RateLimiter getVoiceDestinationDailyLimiter() {
    return voiceDestinationDailyLimiter;
  }

  public RateLimiter getVerifyLimiter() {
    return verifyLimiter;
  }

  public RateLimiter getPinLimiter() {
    return pinLimiter;
  }

  public RateLimiter getTurnLimiter() {
    return turnLimiter;
  }

  public RateLimiter getProfileLimiter() {
    return profileLimiter;
  }

  public RateLimiter getStickerPackLimiter() {
    return stickerPackLimiter;
  }

  public RateLimiter getUsernameLookupLimiter() {
    return usernameLookupLimiter;
  }

  public RateLimiter getUsernameSetLimiter() {
    return usernameSetLimiter;
  }

  private CardinalityRateLimiter createUnsealedSenderLimiter(FaultTolerantRedisCluster cacheCluster, FaultTolerantRedisCluster secondaryCacheCluster, CardinalityRateLimitConfiguration configuration) {
    return new CardinalityRateLimiter(cacheCluster, secondaryCacheCluster, "unsealedSender", configuration.getTtl(), configuration.getTtlJitter(), configuration.getMaxCardinality());
  }

  private RateLimiter createUnsealedIpLimiter(FaultTolerantRedisCluster cacheCluster,
                                              @Nullable FaultTolerantRedisCluster secondaryCacheCluster,
                                              RateLimitConfiguration configuration)
  {
    return createLimiter(cacheCluster, secondaryCacheCluster, configuration, "unsealedIp");
  }

  private RateLimiter createLimiter(FaultTolerantRedisCluster cacheCluster, @Nullable FaultTolerantRedisCluster secondaryCacheCluster, RateLimitConfiguration configuration, String name) {
    return new RateLimiter(cacheCluster, secondaryCacheCluster, name,
                           configuration.getBucketSize(),
                           configuration.getLeakRatePerMinute());
  }
}
