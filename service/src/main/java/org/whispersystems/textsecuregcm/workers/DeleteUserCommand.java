/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static com.codahale.metrics.MetricRegistry.name;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsDynamoDb;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.AccountsManager.DeletionReason;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.storage.Messages;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.storage.Usernames;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.util.Constants;

public class DeleteUserCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(DeleteUserCommand.class);

  public DeleteUserCommand() {
    super(new Application<WhisperServerConfiguration>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment)
          throws Exception
      {

      }
    }, "rmuser", "remove user");
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);
    subparser.addArgument("-u", "--user")
             .dest("user")
             .type(String.class)
             .required(true)
             .help("The user to remove");
  }

  @Override
  protected void run(Environment environment, Namespace namespace,
                     WhisperServerConfiguration configuration)
      throws Exception
  {
    try {
      String[] users = namespace.getString("user").split(",");

      environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      JdbiFactory           jdbiFactory                 = new JdbiFactory();
      Jdbi                  accountJdbi                 = jdbiFactory.build(environment, configuration.getAccountsDatabaseConfiguration(), "accountdb");
      Jdbi                  messageJdbi                 = jdbiFactory.build(environment, configuration.getMessageStoreConfiguration(), "messagedb");
      FaultTolerantDatabase accountDatabase             = new FaultTolerantDatabase("account_database_delete_user", accountJdbi, configuration.getAccountsDatabaseConfiguration().getCircuitBreakerConfiguration());
      FaultTolerantDatabase messageDatabase             = new FaultTolerantDatabase("message_database", messageJdbi, configuration.getMessageStoreConfiguration().getCircuitBreakerConfiguration());
      ClientResources       redisClusterClientResources = ClientResources.builder().build();

      AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder
              .standard()
              .withRegion(configuration.getMessageDynamoDbConfiguration().getRegion())
              .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(((int) configuration.getMessageDynamoDbConfiguration().getClientExecutionTimeout().toMillis()))
                                                                .withRequestTimeout((int) configuration.getMessageDynamoDbConfiguration().getClientRequestTimeout().toMillis()))
              .withCredentials(InstanceProfileCredentialsProvider.getInstance());

      AmazonDynamoDBClientBuilder keysDynamoDbClientBuilder = AmazonDynamoDBClientBuilder
              .standard()
              .withRegion(configuration.getKeysDynamoDbConfiguration().getRegion())
              .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(((int) configuration.getKeysDynamoDbConfiguration().getClientExecutionTimeout().toMillis()))
                                                                .withRequestTimeout((int) configuration.getKeysDynamoDbConfiguration().getClientRequestTimeout().toMillis()))
              .withCredentials(InstanceProfileCredentialsProvider.getInstance());

      AmazonDynamoDBClientBuilder accountsDynamoDbClientBuilder = AmazonDynamoDBClientBuilder
          .standard()
          .withRegion(configuration.getAccountsDynamoDbConfiguration().getRegion())
          .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(((int) configuration.getAccountsDynamoDbConfiguration().getClientExecutionTimeout().toMillis()))
              .withRequestTimeout((int) configuration.getAccountsDynamoDbConfiguration().getClientRequestTimeout().toMillis()))
          .withCredentials(InstanceProfileCredentialsProvider.getInstance());

      DynamoDB messageDynamoDb = Constants.DYNAMO_DB ? new DynamoDB(clientBuilder.build()) : null;
      DynamoDB preKeysDynamoDb = Constants.DYNAMO_DB ? new DynamoDB(keysDynamoDbClientBuilder.build()) : null;

      AmazonDynamoDB accountsDynamoDbClient = accountsDynamoDbClientBuilder.build();

      FaultTolerantRedisCluster cacheCluster = new FaultTolerantRedisCluster("main_cache_cluster", configuration.getCacheClusterConfiguration(), redisClusterClientResources);

      ExecutorService keyspaceNotificationDispatchExecutor = environment.lifecycle().executorService(name(getClass(), "keyspaceNotification-%d")).maxThreads(4).build();
      ExecutorService backupServiceExecutor = environment.lifecycle().executorService(name(getClass(), "backupService-%d")).maxThreads(8).minThreads(1).build();
      ExecutorService storageServiceExecutor = environment.lifecycle().executorService(name(getClass(), "storageService-%d")).maxThreads(8).minThreads(1).build();

      ExternalServiceCredentialGenerator backupCredentialsGenerator = new ExternalServiceCredentialGenerator(configuration.getSecureBackupServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);
      ExternalServiceCredentialGenerator storageCredentialsGenerator = new ExternalServiceCredentialGenerator(configuration.getSecureStorageServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);

      DynamicConfigurationManager dynamicConfigurationManager = new DynamicConfigurationManager(configuration.getAppConfig().getApplication(), configuration.getAppConfig().getEnvironment(), configuration.getAppConfig().getConfigurationName());
      dynamicConfigurationManager.start();

      ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

      Accounts                  accounts             = new Accounts(accountDatabase);
      AccountsDynamoDb          accountsDynamoDb     = new AccountsDynamoDb(accountsDynamoDbClient, new DynamoDB(accountsDynamoDbClient), configuration.getAccountsDynamoDbConfiguration().getTableName(), configuration.getAccountsDynamoDbConfiguration().getPhoneNumberTableName());
      Usernames                 usernames            = new Usernames(accountDatabase);
      Profiles                  profiles             = new Profiles(accountDatabase);
      ReservedUsernames         reservedUsernames    = new ReservedUsernames(accountDatabase);
      Messages                  messages             = new Messages(messageDatabase);
      Keys                      keys                 = new Keys(accountDatabase, configuration.getAccountsDatabaseConfiguration().getKeyOperationRetryConfiguration());
      KeysDynamoDb              keysDynamoDb         = Constants.DYNAMO_DB ? new KeysDynamoDb(preKeysDynamoDb, configuration.getKeysDynamoDbConfiguration().getTableName()) : null;
      MessagesDynamoDb          messagesDynamoDb     = Constants.DYNAMO_DB ? new MessagesDynamoDb(messageDynamoDb, configuration.getMessageDynamoDbConfiguration().getTableName(), configuration.getMessageDynamoDbConfiguration().getTimeToLive()) : null;
      ReplicatedJedisPool       redisClient          = new RedisClientFactory("directory_cache_delete_command", configuration.getDirectoryConfiguration().getRedisConfiguration().getUrl(), configuration.getDirectoryConfiguration().getRedisConfiguration().getReplicaUrls(), configuration.getDirectoryConfiguration().getRedisConfiguration().getCircuitBreakerConfiguration()).getRedisClientPool();
      FaultTolerantRedisCluster messageInsertCacheCluster = new FaultTolerantRedisCluster("message_insert_cluster", configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
      FaultTolerantRedisCluster messageReadDeleteCluster = new FaultTolerantRedisCluster("message_read_delete_cluster", configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
      FaultTolerantRedisCluster metricsCluster       = new FaultTolerantRedisCluster("metrics_cluster", configuration.getMetricsClusterConfiguration(), redisClusterClientResources);
      SecureBackupClient        secureBackupClient   = new SecureBackupClient(backupCredentialsGenerator, backupServiceExecutor, configuration.getSecureBackupServiceConfiguration());
      SecureStorageClient       secureStorageClient  = new SecureStorageClient(storageCredentialsGenerator, storageServiceExecutor, configuration.getSecureStorageServiceConfiguration());
      MessagesCache             messagesCache        = new MessagesCache(messageInsertCacheCluster, messageReadDeleteCluster, keyspaceNotificationDispatchExecutor);
      PushLatencyManager        pushLatencyManager   = new PushLatencyManager(metricsCluster);
      DirectoryQueue            directoryQueue       = new DirectoryQueue  (configuration.getDirectoryConfiguration().getSqsConfiguration());
      DirectoryManager          directory            = new DirectoryManager(redisClient);
      UsernamesManager          usernamesManager     = new UsernamesManager(usernames, reservedUsernames, cacheCluster);
      ProfilesManager           profilesManager      = new ProfilesManager(profiles, cacheCluster);
      MessagesManager           messagesManager      = new MessagesManager(messages, messagesDynamoDb, messagesCache, pushLatencyManager);
      AccountsManager           accountsManager      = new AccountsManager(accounts, accountsDynamoDb, directory, cacheCluster, directoryQueue, keys, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);

      for (String user: users) {
        Optional<Account> account = accountsManager.get(user);

        if (account.isPresent()) {
          accountsManager.delete(account.get(), DeletionReason.ADMIN_DELETED);
          logger.warn("Removed " + account.get().getNumber());
        } else {
          logger.warn("Account not found");
        }
      }
    } catch (Exception ex) {
      logger.warn("Removal Exception", ex);
      throw new RuntimeException(ex);
    }
  }
}
