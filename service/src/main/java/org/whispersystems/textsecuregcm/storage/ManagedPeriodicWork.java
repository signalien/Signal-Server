/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

import static com.codahale.metrics.MetricRegistry.name;

public abstract class ManagedPeriodicWork implements Managed {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String FUTURE_DONE_GAUGE_NAME = "futureDone";

  private final ManagedPeriodicWorkLock lock;
  private final Duration workerTtl;
  private final Duration runInterval;
  private final String workerId;
  private final ScheduledExecutorService executorService;

  @Nullable
  private ScheduledFuture<?> scheduledFuture;

  public ManagedPeriodicWork(final ManagedPeriodicWorkLock lock, final Duration workerTtl, final Duration runInterval, final ScheduledExecutorService scheduledExecutorService) {
    this.lock = lock;
    this.workerTtl = workerTtl;
    this.runInterval = runInterval;
    this.workerId = UUID.randomUUID().toString();
    this.executorService = scheduledExecutorService;
  }

  abstract protected void doPeriodicWork() throws Exception;

  @Override
  public synchronized void start() throws Exception {

    if (scheduledFuture != null) {
      return;
    }

    scheduledFuture = executorService.scheduleAtFixedRate(() -> {
      try {
        execute();
      } catch (final Exception e) {
        logger.warn("Error in execution", e);

        // wait a bit, in case the error is caused by external instability
        Util.sleep(10_000);
      }
    }, 0, runInterval.getSeconds(), TimeUnit.SECONDS);

    Metrics.gauge(name(getClass(), FUTURE_DONE_GAUGE_NAME), scheduledFuture, future -> future.isDone() ? 1 : 0);
  }

  @Override
  public synchronized void stop() throws Exception {

    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);

      boolean terminated = false;
      while (!terminated) {
        try {
          scheduledFuture.get(5, TimeUnit.MINUTES);
          terminated = true;
        } catch (final TimeoutException e) {
          logger.warn("worker not yet terminated");
        } catch (final Exception e) {
          logger.warn("worker terminated exceptionally", e);
          terminated = true;
        }
      }
    }
  }

  private void execute() {

    if (lock.claimActiveWork(workerId, workerTtl)) {
      try {
        logger.info("Starting execution");
        doPeriodicWork();
        logger.info("Execution complete");

      } catch (final Exception e) {
        logger.warn("Periodic work failed", e);

        // wait a bit, in case the error is caused by external instability
        Util.sleep(10_000);

      } finally {
        lock.releaseActiveWork(workerId);
      }
    }
  }
}
