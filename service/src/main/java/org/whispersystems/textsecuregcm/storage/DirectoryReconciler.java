/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse.Status;
import org.whispersystems.textsecuregcm.storage.DirectoryManager.BatchOperationHandle;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import javax.ws.rs.ProcessingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler extends AccountDatabaseCrawlerListener {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);
  private static final String SEND_TIMER_NAME = name(DirectoryReconciler.class, "sendRequest");

  private final String                        replicationName;
  private final boolean                       primary;
  private final DirectoryManager              directoryManager;
  private final DirectoryReconciliationClient reconciliationClient;

  private boolean useV3Endpoints;

  public DirectoryReconciler(String replicationName, boolean primary, DirectoryReconciliationClient reconciliationClient, DirectoryManager directoryManager) {
    this.primary              = primary;
    this.directoryManager     = directoryManager;
    this.reconciliationClient = reconciliationClient;
    this.replicationName      = replicationName;
  }

  @Override
  public void onCrawlStart() {
  }

  @Override
  public void onCrawlEnd(Optional<UUID> fromUuid) {
    if (useV3Endpoints) {
      reconciliationClient.complete();
    } else {
      if (!Constants.DYNAMO_DB) return;
      final DirectoryReconciliationRequest request = new DirectoryReconciliationRequest(fromUuid.orElse(null), null,
          Collections.emptyList());
      sendAdditions(request);
    }
  }

  @Override
  protected void onCrawlChunk(final Optional<UUID> fromUuid, final List<Account> accounts)
      throws AccountDatabaseCrawlerRestartException {

    if (primary) {
      updateDirectoryCache(accounts);
    }
    if (!Constants.DYNAMO_DB) return;

    final DirectoryReconciliationRequest addUsersRequest;
    final DirectoryReconciliationRequest deleteUsersRequest;
    {
      final List<DirectoryReconciliationRequest.User> addedUsers = new ArrayList<>(accounts.size());
      final List<DirectoryReconciliationRequest.User> deletedUsers = new ArrayList<>(accounts.size());

      accounts.forEach(account -> {
        if (account.shouldBeVisibleInDirectory()) {
          addedUsers.add(new DirectoryReconciliationRequest.User(account.getUuid(), account.getNumber()));
        } else {
          deletedUsers.add(new DirectoryReconciliationRequest.User(account.getUuid(), account.getNumber()));
        }
      });

      final Optional<UUID> toUuid;
      if (!accounts.isEmpty()) {
        toUuid = Optional.of(accounts.get(accounts.size() - 1).getUuid());
      } else {
        toUuid = Optional.empty();
      }

      addUsersRequest = new DirectoryReconciliationRequest(fromUuid.orElse(null), toUuid.orElse(null), addedUsers);
      deleteUsersRequest = new DirectoryReconciliationRequest(null, null, deletedUsers);
    }

    final DirectoryReconciliationResponse addUsersResponse = sendAdditions(addUsersRequest);
    final DirectoryReconciliationResponse deleteUsersResponse = sendDeletes(deleteUsersRequest);

    if (addUsersResponse.getStatus() == DirectoryReconciliationResponse.Status.MISSING
        || deleteUsersResponse.getStatus() == Status.MISSING) {

      throw new AccountDatabaseCrawlerRestartException("directory reconciler missing");
    }
  }

  private DirectoryReconciliationResponse sendDeletes(final DirectoryReconciliationRequest request) {
    if (useV3Endpoints) {
      return sendRequest(request, reconciliationClient::delete, "delete");
    }

    return new DirectoryReconciliationResponse(DirectoryReconciliationResponse.Status.OK);
  }

  private DirectoryReconciliationResponse sendAdditions(final DirectoryReconciliationRequest request) {

    if (useV3Endpoints) {
      return sendRequest(request, reconciliationClient::sendChunkV3, "add");
    }

    return sendRequest(request, reconciliationClient::sendChunk, "add_v2");
  }

  private DirectoryReconciliationResponse sendRequest(final DirectoryReconciliationRequest request,
      final Function<DirectoryReconciliationRequest, DirectoryReconciliationResponse> requestHandler,
      final String context) {

    return Metrics.timer(SEND_TIMER_NAME, "context", context, "replication", replicationName)
        .record(() -> {
          try {
            final DirectoryReconciliationResponse response = requestHandler.apply(request);

            if (response.getStatus() != DirectoryReconciliationResponse.Status.OK) {
              logger.warn("reconciliation error: " + response.getStatus());
            }
            return response;
          } catch (ProcessingException ex) {
            logger.warn("request error: ", ex);
            throw new ProcessingException(ex);
          }
        });
  }

  public void setUseV3Endpoints(final boolean useV3Endpoints) {
    this.useV3Endpoints = useV3Endpoints;
  }

  private void updateDirectoryCache(List<Account> accounts) {

    BatchOperationHandle batchOperation = directoryManager.startBatchOperation();

    try {
      for (Account account : accounts) {
        if (account.isEnabled() && account.isDiscoverableByPhoneNumber()) {
          byte[]        token         = Util.getContactToken(account.getNumber());
          ClientContact clientContact = new ClientContact(token, null, true, true);
          directoryManager.add(batchOperation, clientContact);
        } else {
          directoryManager.remove(batchOperation, account.getNumber());
        }
      }
    } finally {
      directoryManager.stopBatchOperation(batchOperation);
    }
  }
}
