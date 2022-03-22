/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;

import java.util.Optional;

public class PendingAccountsManager {

  private final PendingAccounts pendingAccounts;

  public PendingAccountsManager(final PendingAccounts pendingAccounts) {
    this.pendingAccounts = pendingAccounts;
  }

  public void store(String number, StoredVerificationCode code) {
    pendingAccounts.insert(number, code);
  }

  public void remove(String number) {
    pendingAccounts.remove(number);
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    return pendingAccounts.findForNumber(number);
  }
}
