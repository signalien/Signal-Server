/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.basic.BasicCredentials;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Util;

import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;

public class BaseAccountAuthenticator {

  private static final String AUTHENTICATION_COUNTER_NAME = name(BaseAccountAuthenticator.class, "authentication");
  private static final String AUTHENTICATION_SUCCEEDED_TAG_NAME = "succeeded";
  private static final String AUTHENTICATION_FAILURE_REASON_TAG_NAME = "reason";
  private static final String AUTHENTICATION_ENABLED_REQUIRED_TAG_NAME = "enabledRequired";
  private static final String AUTHENTICATION_CREDENTIAL_TYPE_TAG_NAME = "credentialType";

  private static final String DAYS_SINCE_LAST_SEEN_DISTRIBUTION_NAME = name(BaseAccountAuthenticator.class, "daysSinceLastSeen");
  private static final String IS_PRIMARY_DEVICE_TAG = "isPrimary";

  private final AccountsManager accountsManager;
  private final Clock           clock;

  public BaseAccountAuthenticator(AccountsManager accountsManager) {
    this(accountsManager, Clock.systemUTC());
  }

  @VisibleForTesting
  public BaseAccountAuthenticator(AccountsManager accountsManager, Clock clock) {
    this.accountsManager = accountsManager;
    this.clock           = clock;
  }

  public Optional<Account> authenticate(BasicCredentials basicCredentials, boolean enabledRequired) {
    boolean succeeded = false;
    String failureReason = null;
    String credentialType = null;

    try {
      AuthorizationHeader authorizationHeader = AuthorizationHeader.fromUserAndPassword(basicCredentials.getUsername(), basicCredentials.getPassword());
      Optional<Account>   account             = accountsManager.get(authorizationHeader.getIdentifier());

      credentialType = authorizationHeader.getIdentifier().hasNumber() ? "e164" : "uuid";

      if (account.isEmpty()) {
        failureReason = "noSuchAccount";
        return Optional.empty();
      }

      Optional<Device> device = account.get().getDevice(authorizationHeader.getDeviceId());

      if (device.isEmpty()) {
        failureReason = "noSuchDevice";
        return Optional.empty();
      }

      if (enabledRequired) {
        if (!device.get().isEnabled()) {
          failureReason = "deviceDisabled";
          return Optional.empty();
        }

        if (!account.get().isEnabled()) {
          failureReason = "accountDisabled";
          return Optional.empty();
        }
      }

      if (device.get().getAuthenticationCredentials().verify(basicCredentials.getPassword())) {
        succeeded = true;
        account.get().setAuthenticatedDevice(device.get());
        return Optional.of(updateLastSeen(account.get(), device.get()));
      }

      return Optional.empty();
    } catch (IllegalArgumentException | InvalidAuthorizationHeaderException iae) {
      failureReason = "invalidHeader";
      return Optional.empty();
    } finally {
      Tags tags = Tags.of(
          AUTHENTICATION_SUCCEEDED_TAG_NAME, String.valueOf(succeeded),
          AUTHENTICATION_ENABLED_REQUIRED_TAG_NAME, String.valueOf(enabledRequired));

      if (StringUtils.isNotBlank(failureReason)) {
        tags = tags.and(AUTHENTICATION_FAILURE_REASON_TAG_NAME, failureReason);
      }

      if (StringUtils.isNotBlank(credentialType)) {
        tags = tags.and(AUTHENTICATION_CREDENTIAL_TYPE_TAG_NAME, credentialType);
      }

      Metrics.counter(AUTHENTICATION_COUNTER_NAME, tags).increment();
    }
  }

  @VisibleForTesting
  public Account updateLastSeen(Account account, Device device) {
    final long lastSeenOffsetSeconds   = Math.abs(account.getUuid().getLeastSignificantBits()) % ChronoUnit.DAYS.getDuration().toSeconds();
    final long todayInMillisWithOffset = Util.todayInMillisGivenOffsetFromNow(clock, Duration.ofSeconds(lastSeenOffsetSeconds).negated());

    if (device.getLastSeen() < todayInMillisWithOffset) {
      Metrics.summary(DAYS_SINCE_LAST_SEEN_DISTRIBUTION_NAME, IS_PRIMARY_DEVICE_TAG, String.valueOf(device.isMaster()))
          .record(Duration.ofMillis(todayInMillisWithOffset - device.getLastSeen()).toDays());

      device.setLastSeen(Util.todayInMillis(clock));
      return accountsManager.updateDevice(account, device.getId(), d -> d.setLastSeen(Util.todayInMillis(clock)));
    }

    return account;
  }

}
