/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.sentry.AirbyteSentry;
import io.airbyte.integrations.destination.jdbc.StagingSqlOperations;
import io.airbyte.integrations.destination.s3.S3DestinationConfig;
import io.airbyte.integrations.destination.s3.csv.CsvSheetGenerator;
import io.airbyte.integrations.destination.s3.csv.S3CsvFormatConfig;
import io.airbyte.integrations.destination.s3.csv.S3CsvWriter;
import io.airbyte.integrations.destination.s3.csv.StagingDatabaseCsvSheetGenerator;
import io.airbyte.integrations.destination.s3.util.S3OutputPathHelper;
import io.airbyte.integrations.destination.s3.util.S3StreamTransferManagerHelper;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.SyncMode;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeS3StagingSqlOperations extends SnowflakeSqlOperations implements StagingSqlOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSqlOperations.class);
  private static final int DEFAULT_UPLOAD_THREADS = 10; // The S3 cli uses 10 threads by default.
  private static final int DEFAULT_QUEUE_CAPACITY = DEFAULT_UPLOAD_THREADS;

  protected final AmazonS3 s3Client;
  protected final S3DestinationConfig s3Config;

  public SnowflakeS3StagingSqlOperations(final AmazonS3 s3Client,
                                         final S3DestinationConfig s3Config) {
    this.s3Client = s3Client;
    this.s3Config = s3Config;
  }

  @Override
  public void insertRecordsInternal(final JdbcDatabase database,
                                    final List<AirbyteRecordMessage> records,
                                    final String schemaName,
                                    final String stage) {
    LOGGER.info("Writing {} records to {}", records.size(), stage);
    if (records.isEmpty()) {
      return;
    }
    try {
      loadDataIntoStage(database, stage, records);
    } catch (final Exception e) {
      LOGGER.error("Failed to upload records into stage {}", stage, e);
      throw new RuntimeException(e);
    }
  }

  private void loadDataIntoStage(final JdbcDatabase database, final String stage, final List<AirbyteRecordMessage> records) {
    final Long partSize = s3Config.getFormatConfig() != null ? s3Config.getFormatConfig().getPartSize() : null;
    final String bucket = s3Config.getBucketName();
    final StreamTransferManager uploadManager = S3StreamTransferManagerHelper
        .getDefault(bucket, stage, s3Client, partSize)
      .numUploadThreads(DEFAULT_UPLOAD_THREADS).queueCapacity(DEFAULT_QUEUE_CAPACITY);
    // We only need one output stream as we only have one input stream. This is reasonably performant.
    final MultiPartOutputStream outputStream = uploadManager.getMultiPartOutputStreams().get(0);
    boolean hasFailed = false;
    try (outputStream; CSVPrinter csvPrinter = new CSVPrinter(new PrintWriter(outputStream, true, StandardCharsets.UTF_8), CSVFormat.DEFAULT)) {
      final CsvSheetGenerator csvSheetGenerator = new StagingDatabaseCsvSheetGenerator();
      createStageIfNotExists(database, stage);
      for (final AirbyteRecordMessage recordMessage : records) {
        final var id = UUID.randomUUID();
        csvPrinter.printRecord(csvSheetGenerator.getDataRow(id, recordMessage));
      }
    } catch (Exception e) {
      LOGGER.error("Failed to load data into stage {}", stage, e);
      hasFailed = true;
    }
    if (hasFailed) {
      uploadManager.complete();
    } else {
      uploadManager.abort();
    }
  }

  @Override
  public void createStageIfNotExists(final JdbcDatabase database, final String stageName) {
    final String bucket = s3Config.getBucketName();
    if (!s3Client.doesBucketExistV2(bucket)) {
      LOGGER.info("Bucket {} does not exist; creating...", bucket);
      s3Client.createBucket(bucket);
      LOGGER.info("Bucket {} has been created.", bucket);
    }
  }

  @Override
  public void copyIntoTmpTableFromStage(final JdbcDatabase database, final String stageName, final String dstTableName, final String schemaName) {
    LOGGER.info("Starting copy to tmp table from stage: {} in destination from stage: {}, schema: {}, .", dstTableName, stageName, schemaName);
    final var copyQuery = String.format(
        "COPY INTO %s.%s FROM '%s' "
            + "CREDENTIALS=(aws_key_id='%s' aws_secret_key='%s') "
            + "file_format = (type = csv field_delimiter = ',' skip_header = 0 FIELD_OPTIONALLY_ENCLOSED_BY = '\"');",
        schemaName,
        dstTableName,
        generateBucketPath(stageName),
        s3Config.getAccessKeyId(),
        s3Config.getSecretAccessKey());
    Exceptions.toRuntime(() -> database.execute(copyQuery));
    LOGGER.info("Copy to tmp table {}.{} in destination complete.", schemaName, dstTableName);
  }

  private String generateBucketPath(final String stage) {
    return "s3://" + s3Config.getBucketName() + "/" + stage + "/";
  }

  @Override
  public void dropStageIfExists(final JdbcDatabase database, final String stageName) {
    final String bucket = s3Config.getBucketName();
    if (s3Client.doesObjectExist(bucket, stageName)) {
      LOGGER.info("Removing stage {}...", stageName);
      s3Client.deleteObject(bucket, stageName);
      LOGGER.info("Stage object {} has been deleted...", stageName);
    }
  }

  @Override
  public void cleanUpStage(final JdbcDatabase database, final String path) {
    dropStageIfExists(database, path);
  }
}

