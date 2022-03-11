package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.UUID;
import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.jdbi.v3.core.Jdbi;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;

class MessagesManagerTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("messagedb.xml"));

  private final Messages messages = new Messages(new FaultTolerantDatabase("messages-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
  private final MessagesDynamoDb messagesDynamoDb = mock(MessagesDynamoDb.class);
  private final MessagesCache messagesCache = mock(MessagesCache.class);
  private final PushLatencyManager pushLatencyManager = mock(PushLatencyManager.class);
  private final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);

  private final MessagesManager messagesManager = new MessagesManager(messages, messagesDynamoDb, messagesCache,
      pushLatencyManager, reportMessageManager);

  @Test
  void insert() {
    final String sourceNumber = "+12025551212";
    final Envelope message = Envelope.newBuilder()
        .setSource(sourceNumber)
        .setSourceUuid(UUID.randomUUID().toString())
        .build();

    final UUID destinationUuid = UUID.randomUUID();

    messagesManager.insert(destinationUuid, 1L, message);

    verify(reportMessageManager).store(eq(sourceNumber), any(UUID.class));

    final Envelope syncMessage = Envelope.newBuilder(message)
        .setSourceUuid(destinationUuid.toString())
        .build();

    messagesManager.insert(destinationUuid, 1L, syncMessage);

    verifyNoMoreInteractions(reportMessageManager);
  }
}
