/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for upgrade/downgrade operations using pre-created fixture tables
 * from different Hudi releases. Tests round-trip operations: upgrade one version up,
 * then downgrade back to original version.
 */
public class TestUpgradeDowngradeFixtures extends HoodieSparkClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeDowngradeFixtures.class);
  private static final String FIXTURES_BASE_PATH = "/upgrade-downgrade-fixtures/mor-tables/";
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    initSparkContexts();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
  }

  @ParameterizedTest
  @MethodSource("fixtureVersions")
  public void testRoundTripUpgradeDowngrade(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing round-trip upgrade/downgrade for version {}", originalVersion);
    
    // Load fixture table
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    assertEquals(originalVersion, originalMetaClient.getTableConfig().getTableVersion(),
        "Fixture table should be at expected version");
    
    // Calculate target version (next version up)
    HoodieTableVersion targetVersion = getNextVersion(originalVersion);
    if (targetVersion == null) {
      LOG.info("Skipping upgrade test for version {} (no higher version available)", originalVersion);
      return;
    }
    
    // Create write config for upgrade operations
    HoodieWriteConfig config = createUpgradeWriteConfig(originalMetaClient, true);
    
    // Step 1: Upgrade to next version
    LOG.info("Step 1: Upgrading from {} to {}", originalVersion, targetVersion);
    new UpgradeDowngrade(originalMetaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Create fresh meta client to read updated table configuration after upgrade
    HoodieTableMetaClient upgradedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    assertTableVersionOnDataAndMetadataTable(upgradedMetaClient, targetVersion);
    validateTableIntegrity(upgradedMetaClient, "after upgrade");
    
    // Step 2: Downgrade back to original version
    LOG.info("Step 2: Downgrading from {} back to {}", targetVersion, originalVersion);
    new UpgradeDowngrade(upgradedMetaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(originalVersion, null);
    
    // Create fresh meta client to read updated table configuration after downgrade
    HoodieTableMetaClient finalMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(upgradedMetaClient.getBasePath())
        .build();
    assertTableVersionOnDataAndMetadataTable(finalMetaClient, originalVersion);
    validateTableIntegrity(finalMetaClient, "after round-trip");
    
    LOG.info("Successfully completed round-trip test for version {}", originalVersion);
  }

  @ParameterizedTest
  @MethodSource("fixtureVersions")
  public void testAutoUpgradeDisabled(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing auto-upgrade disabled for version {}", originalVersion);
    
    // Load fixture table
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    
    // Calculate target version (next version up)
    HoodieTableVersion targetVersion = getNextVersion(originalVersion);
    if (targetVersion == null) {
      LOG.info("Skipping auto-upgrade test for version {} (no higher version available)", originalVersion);
      return;
    }
    
    // Create write config with auto-upgrade disabled
    HoodieWriteConfig config = createUpgradeWriteConfig(originalMetaClient, false);
    
    // Attempt upgrade with auto-upgrade disabled
    new UpgradeDowngrade(originalMetaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Create fresh meta client to validate that version remained unchanged 
    HoodieTableMetaClient unchangedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    assertEquals(originalVersion, unchangedMetaClient.getTableConfig().getTableVersion(),
        "Table version should remain unchanged when auto-upgrade is disabled");
    
    LOG.info("Auto-upgrade disabled test passed for version {}", originalVersion);
  }

  @ParameterizedTest
  @MethodSource("fixtureVersions") 
  public void testRollbackAndCompactionBehavior(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing rollback and compaction behavior for version {}", originalVersion);
    
    // Load fixture table
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    
    // Calculate target version
    HoodieTableVersion targetVersion = getNextVersion(originalVersion);
    if (targetVersion == null) {
      LOG.info("Skipping rollback/compaction test for version {} (no higher version available)", originalVersion);
      return;
    }
    
    // Create write config for upgrade operations
    HoodieWriteConfig config = createUpgradeWriteConfig(originalMetaClient, true);
    
    // Count initial timeline state
    int initialPendingCommits = originalMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    int initialCompletedCommits = originalMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    
    // Perform upgrade
    new UpgradeDowngrade(originalMetaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Create fresh meta client to validate timeline state after upgrade
    HoodieTableMetaClient upgradedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    
    // Verify rollback behavior - pending commits should be cleaned up or reduced
    int finalPendingCommits = upgradedMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    assertTrue(finalPendingCommits <= initialPendingCommits,
        "Pending commits should be cleaned up or reduced after upgrade");
    
    // Verify we still have completed commits
    int finalCompletedCommits = upgradedMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    assertTrue(finalCompletedCommits >= initialCompletedCommits,
        "Completed commits should be preserved or increased after upgrade");
    
    LOG.info("Rollback and compaction behavior validated for version {}", originalVersion);
  }

  /**
   * Load a fixture table from resources and copy it to a temporary location for testing.
   */
  private HoodieTableMetaClient loadFixtureTable(HoodieTableVersion version) throws IOException {
    String fixtureName = getFixtureName(version);
    String resourcePath = FIXTURES_BASE_PATH + fixtureName;
    
    // Copy fixture from resources to temp directory
    copyFixtureToTempDir(resourcePath, tempDir.toString());
    
    // Initialize meta client for the copied fixture
    metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(tempDir.toString())
        .build();
    
    LOG.info("Loaded fixture table {} at version {}", fixtureName, metaClient.getTableConfig().getTableVersion());
    return metaClient;
  }

  /**
   * Copy fixture table from resources to temporary directory.
   */
  private void copyFixtureToTempDir(String resourcePath, String tempPath) throws IOException {
    // Get the resource URL
    java.net.URL resourceURL = getClass().getResource(resourcePath);
    assertNotNull(resourceURL, "Fixture not found at: " + resourcePath);
    
    // Use FileSystem to copy the directory contents recursively
    FileSystem fs = FileSystem.get(storageConf.unwrap());
    Path sourcePath = new Path(resourceURL.getPath());
    Path targetPath = new Path(tempPath);
    
    // Create the target directory if it doesn't exist
    fs.mkdirs(targetPath);
    
    // Copy all files and subdirectories from source to target
    org.apache.hadoop.fs.FileStatus[] fileStatuses = fs.listStatus(sourcePath);
    for (org.apache.hadoop.fs.FileStatus status : fileStatuses) {
      Path srcFile = status.getPath();
      Path dstFile = new Path(targetPath, srcFile.getName());
      org.apache.hadoop.fs.FileUtil.copy(fs, srcFile, fs, dstFile, false, storageConf.unwrap());
    }
    
    LOG.debug("Copied fixture contents from {} to {}", resourcePath, tempPath);
  }

  /**
   * Create write config for upgrade/downgrade operations.
   */
  private HoodieWriteConfig createUpgradeWriteConfig(HoodieTableMetaClient metaClient, boolean autoUpgrade) {
    Properties props = new Properties();
    props.putAll(metaClient.getTableConfig().getProps());
    
    return HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath().toString())
        .withAutoUpgradeVersion(autoUpgrade)
        .withProps(props)
        .withTimelineLayoutVersion(metaClient.getTableConfig().getTimelineLayoutVersion().get().getVersion())
        .build();
  }

  /**
   * Validate table integrity after upgrade/downgrade operations.
   */
  private void validateTableIntegrity(HoodieTableMetaClient metaClient, String stage) {
    LOG.debug("Validating table integrity {}", stage);
    
    // Basic validation
    assertNotNull(metaClient.getTableConfig(), "Table config should exist");
    assertEquals(HoodieTableType.MERGE_ON_READ, metaClient.getTableType(),
        "Table should remain MOR type");
    
    // Timeline validation
    assertTrue(metaClient.getCommitsTimeline().countInstants() > 0,
        "Timeline should have commits");
    
    // Storage path validation
    try {
      assertTrue(metaClient.getStorage().exists(metaClient.getBasePath()),
          "Table base path should exist");
      assertTrue(metaClient.getStorage().exists(metaClient.getMetaPath()),
          "Meta path should exist");
    } catch (IOException e) {
      throw new RuntimeException("Failed to validate storage paths", e);
    }
  }

  /**
   * Get the next version up from the current version.
   */
  private HoodieTableVersion getNextVersion(HoodieTableVersion current) {
    switch (current) {
      case FOUR:
        return HoodieTableVersion.FIVE;
      case FIVE:
        return HoodieTableVersion.SIX;
      case SIX:
        // even though there is a table version 7, this is not an official release and serves as a bridge
        // so the next version should be 8
        return HoodieTableVersion.EIGHT;
      case EIGHT:
        return HoodieTableVersion.NINE;
      case NINE:
        return null; // No higher version available
      default:
        return null;
    }
  }

  /**
   * Get fixture directory name for a given table version.
   */
  private String getFixtureName(HoodieTableVersion version) {
    switch (version) {
      case FOUR:
        return "hudi-v4-table";
      case FIVE:
        return "hudi-v5-table";
      case SIX:
        return "hudi-v6-table";
      case EIGHT:
        return "hudi-v8-table";
      case NINE:
        return "hudi-v9-table";
      default:
        throw new IllegalArgumentException("Unsupported fixture version: " + version);
    }
  }

  /**
   * Provide test parameters for fixture versions.
   */
  private static Stream<Arguments> fixtureVersions() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.FOUR),   // Hudi 0.11.1
        Arguments.of(HoodieTableVersion.FIVE),   // Hudi 0.12.2
        Arguments.of(HoodieTableVersion.SIX),    // Hudi 0.14
        Arguments.of(HoodieTableVersion.EIGHT),  // Hudi 1.0.2
        Arguments.of(HoodieTableVersion.NINE)    // Hudi 1.1
    );
  }

  /**
   * Assert table version on both data table and metadata table (if exists).
   * Adapted from TestUpgradeDowngrade.assertTableVersionOnDataAndMetadataTable().
   */
  private void assertTableVersionOnDataAndMetadataTable(
      HoodieTableMetaClient metaClient, HoodieTableVersion expectedVersion) throws IOException {
    assertTableVersion(metaClient, expectedVersion);

    if (expectedVersion.versionCode() >= HoodieTableVersion.FOUR.versionCode()) {
      StoragePath metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
      if (metaClient.getStorage().exists(metadataTablePath)) {
        LOG.info("Verifying metadata table version at: {}", metadataTablePath);
        HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
            .setConf(metaClient.getStorageConf().newInstance()).setBasePath(metadataTablePath).build();
        assertTableVersion(mdtMetaClient, expectedVersion);
      } else {
        LOG.info("Metadata table does not exist at: {}", metadataTablePath);
      }
    }
  }

  /**
   * Assert table version by checking both in-memory config and persisted properties file.
   * Adapted from TestUpgradeDowngrade.assertTableVersion().
   */
  private void assertTableVersion(
      HoodieTableMetaClient metaClient, HoodieTableVersion expectedVersion) throws IOException {
    assertEquals(expectedVersion.versionCode(),
        metaClient.getTableConfig().getTableVersion().versionCode());
    StoragePath propertyFile = new StoragePath(
        metaClient.getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    // Load the properties and verify
    InputStream inputStream = metaClient.getStorage().open(propertyFile);
    HoodieConfig config = new HoodieConfig();
    config.getProps().load(inputStream);
    inputStream.close();
    assertEquals(Integer.toString(expectedVersion.versionCode()),
        config.getString(HoodieTableConfig.VERSION));
  }
}
