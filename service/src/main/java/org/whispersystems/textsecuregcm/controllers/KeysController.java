/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseItem;
import org.whispersystems.textsecuregcm.entities.PreKeyState;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.limits.PreKeyRateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeException;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.KeyRecord;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v2/keys")
public class KeysController {

  private final RateLimiters                rateLimiters;
  private final Keys                        keys;
  private final KeysDynamoDb                keysDynamoDb;
  private final AccountsManager             accounts;
  private final PreKeyRateLimiter           preKeyRateLimiter;

  private final DynamicConfigurationManager dynamicConfigurationManager;
  private final RateLimitChallengeManager rateLimitChallengeManager;

  private static final String PREKEY_REQUEST_COUNTER_NAME = name(KeysController.class, "preKeyGet");
  private static final String RATE_LIMITED_GET_PREKEYS_COUNTER_NAME = name(KeysController.class, "rateLimitedGetPreKeys");

  private static final String SOURCE_COUNTRY_TAG_NAME = "sourceCountry";
  private static final String INTERNATIONAL_TAG_NAME = "international";
  private static final String PREKEY_TARGET_IDENTIFIER_TAG_NAME =  "identifierType";

  public KeysController(RateLimiters rateLimiters, Keys keys, KeysDynamoDb keysDynamoDb, AccountsManager accounts,
      PreKeyRateLimiter preKeyRateLimiter,
      DynamicConfigurationManager dynamicConfigurationManager,
      RateLimitChallengeManager rateLimitChallengeManager) {
    this.rateLimiters                = rateLimiters;
    this.keys                        = keys;
    this.keysDynamoDb                = keysDynamoDb;
    this.accounts                    = accounts;
    this.preKeyRateLimiter           = preKeyRateLimiter;

    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.rateLimitChallengeManager = rateLimitChallengeManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public PreKeyCount getStatus(@Auth Account account) {
    int count = Constants.DYNAMO_DB ? keysDynamoDb.getCount(account, account.getAuthenticatedDevice().get().getId())
                                    : keys.getCount(account, account.getAuthenticatedDevice().get().getId());

    if (count > 0) {
      count = count - 1;
    }

    return new PreKeyCount(count);
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public void setKeys(@Auth DisabledPermittedAccount disabledPermittedAccount, @Valid PreKeyState preKeys)  {
    Account account           = disabledPermittedAccount.getAccount();
    Device  device            = account.getAuthenticatedDevice().get();
    boolean updateAccount     = false;

    if (!preKeys.getSignedPreKey().equals(device.getSignedPreKey())) {
      updateAccount = true;
    }

    if (!preKeys.getIdentityKey().equals(account.getIdentityKey())) {
      updateAccount = true;
    }

    if (updateAccount) {
      account = accounts.update(account, a -> {
        a.getDevice(device.getId()).ifPresent(d -> d.setSignedPreKey(preKeys.getSignedPreKey()));
        a.setIdentityKey(preKeys.getIdentityKey());
      });
    }

    if (Constants.DYNAMO_DB) {
      keysDynamoDb.store(account, device.getId(), preKeys.getPreKeys());
    } else {
      keys.store(account, device.getId(), preKeys.getPreKeys());
    }
  }

  @Timed
  @GET
  @Path("/{identifier}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDeviceKeys(@Auth                                     Optional<Account> account,
                                @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
                                @PathParam("identifier")                  AmbiguousIdentifier targetName,
                                @PathParam("device_id")                   String deviceId,
                                @HeaderParam("User-Agent")                String userAgent)
      throws RateLimitExceededException, RateLimitChallengeException {
    if (!account.isPresent() && !accessKey.isPresent()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    Optional<Account> target = accounts.get(targetName);
    OptionalAccess.verify(account, accessKey, target, deviceId);

    assert(target.isPresent());

    {
      final String sourceCountryCode = account.map(a -> Util.getCountryCode(a.getNumber())).orElse("0");
      final String targetCountryCode = target.map(a -> Util.getCountryCode(a.getNumber())).orElseThrow();

      Metrics.counter(PREKEY_REQUEST_COUNTER_NAME, Tags.of(
          SOURCE_COUNTRY_TAG_NAME, sourceCountryCode,
          INTERNATIONAL_TAG_NAME, String.valueOf(!sourceCountryCode.equals(targetCountryCode)),
          PREKEY_TARGET_IDENTIFIER_TAG_NAME, targetName.hasNumber() ? "number" : "uuid"
      )).increment();
    }

    if (account.isPresent()) {
      rateLimiters.getPreKeysLimiter().validate(account.get().getNumber() + "." + account.get().getAuthenticatedDevice().get().getId() +  "__" + target.get().getNumber() + "." + deviceId);

      try {
        preKeyRateLimiter.validate(account.get());
      } catch (RateLimitExceededException e) {

        final boolean enforceLimit = rateLimitChallengeManager.shouldIssueRateLimitChallenge(userAgent);

        Metrics.counter(RATE_LIMITED_GET_PREKEYS_COUNTER_NAME,
            SOURCE_COUNTRY_TAG_NAME, Util.getCountryCode(account.get().getNumber()),
            "enforced", String.valueOf(enforceLimit))
            .increment();

        if (enforceLimit) {
          throw new RateLimitChallengeException(account.get(), e.getRetryDuration());
        }
      }
    }

    Map<Long, PreKey>        preKeysByDeviceId = getLocalKeys(target.get(), deviceId);
    List<PreKeyResponseItem> responseItems     = new LinkedList<>();

    for (Device device : target.get().getDevices()) {
      if (device.isEnabled() && (deviceId.equals("*") || device.getId() == Long.parseLong(deviceId))) {
        SignedPreKey signedPreKey = device.getSignedPreKey();
        PreKey       preKey       = preKeysByDeviceId.get(device.getId());

        if (signedPreKey != null || preKey != null) {
          responseItems.add(new PreKeyResponseItem(device.getId(), device.getRegistrationId(), signedPreKey, preKey));
        }
      }
    }

    if (responseItems.isEmpty()) return Response.status(404).build();
    else                         return Response.ok().entity(new PreKeyResponse(target.get().getIdentityKey(), responseItems)).build();
  }

  @Timed
  @PUT
  @Path("/signed")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setSignedKey(@Auth Account account, @Valid SignedPreKey signedPreKey) {
    Device device = account.getAuthenticatedDevice().get();

    accounts.updateDevice(account, device.getId(), d -> d.setSignedPreKey(signedPreKey));
  }

  @Timed
  @GET
  @Path("/signed")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<SignedPreKey> getSignedKey(@Auth Account account) {
    Device       device       = account.getAuthenticatedDevice().get();
    SignedPreKey signedPreKey = device.getSignedPreKey();

    if (signedPreKey != null) return Optional.of(signedPreKey);
    else                      return Optional.empty();
  }

  private Map<Long, PreKey> getLocalKeys(Account destination, String deviceIdSelector) {
    try {
      if (deviceIdSelector.equals("*")) {
        if (Constants.DYNAMO_DB) {
          return keysDynamoDb.take(destination);
        } else {
          return keys.take(destination).stream().collect(Collectors.toMap(KeyRecord::getDeviceId, keyRecord -> new PreKey(keyRecord.getKeyId(), keyRecord.getPublicKey())));
        }
      }

      long deviceId = Long.parseLong(deviceIdSelector);

      if (Constants.DYNAMO_DB) {
        return keysDynamoDb.take(destination, deviceId)
                           .map(preKey -> Map.of(deviceId, preKey))
                           .orElse(Collections.emptyMap());
      } else {
        return keys.take(destination, deviceId).stream().collect(Collectors.toMap(KeyRecord::getDeviceId, keyRecord -> new PreKey(keyRecord.getKeyId(), keyRecord.getPublicKey())));
      }
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }
}
