/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.rocks.ImmutableRocksDbConfig;
import org.projectnessie.versioned.persist.rocks.RocksDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.rocks.RocksDbConfig;
import org.projectnessie.versioned.persist.rocks.RocksDbInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class TestExportRocks {

  static DatabaseAdapter rocksDatabaseAdapter;

  static ExportNessieRepo exportNessieRepo;

  @BeforeClass
  public static void beforeClass() throws Exception {

    /**The path given here is wrong , should give the path present in Quarkus server */

    StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

    // Path dir =

    Path rocksDir = (Path) Paths.get("/Users" , "aditya.vemulapalli", "Downloads", "2nessie", "nessie"
    , "servers", "quarkus-server", "nessie-rocksdb");

    // ## RocksDB version store specific configuration
    // #nessie.version.store.rocks.db-path=nessie-rocksdb

    // rocksDir = Files.createTempDirectory(dir , "junit-rocks-export");

    String dbPath = rocksDir.toString();

    RocksDbConfig config  = ImmutableRocksDbConfig
      .builder()
      .dbPath(dbPath)
      .build();

    RocksDbInstance connector = new RocksDbInstance();
    connector.configure(config);
    connector.initialize();

    NonTransactionalDatabaseAdapterConfig rocksDbAdapterConfig = ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build();

    rocksDatabaseAdapter = new RocksDatabaseAdapterFactory()
      .newBuilder()
      .withConnector(connector)
      .withConfig(rocksDbAdapterConfig)
      .build(storeWorker);

    exportNessieRepo = new ExportNessieRepo(rocksDatabaseAdapter);

  }

  @Test
  public void testRepoDesc() {

    String targetDirectory = "/Users/aditya.vemulapalli/Downloads";
    exportNessieRepo.exportRepoDesc(targetDirectory);

    /**Testing the serialized repo desc is correct or not */
    /**The repo desc file must be empty */

    Assertions.assertThat(ExportTestsHelper.fetchBytesInRepoDesc(targetDirectory)).isEqualTo(0);
  }

  @Test
  public void testNamedRefs(){
    String targetDirectory = "/Users/aditya.vemulapalli/Downloads";

    exportNessieRepo.exportNamedRefs(targetDirectory);

    List<ReferenceInfoExport> originalNamedRefsInfoList = ExportTestsHelper.fetchNamedRefsInfoList(rocksDatabaseAdapter);
    List<ReferenceInfoExport> deserializedNamedRefsInfoList = ExportTestsHelper.deserializeNamedRefsInfoList(targetDirectory);

    Assertions.assertThat(originalNamedRefsInfoList.size()).isEqualTo(deserializedNamedRefsInfoList.size());

    for(int i = 0 ; i < originalNamedRefsInfoList.size(); i++)
    {
      Assertions.assertThat(originalNamedRefsInfoList.get(i).referenceName).isEqualTo(deserializedNamedRefsInfoList.get(i).referenceName);

      Assertions.assertThat(originalNamedRefsInfoList.get(i).type).isEqualTo(deserializedNamedRefsInfoList.get(i).type);

      Assertions.assertThat(originalNamedRefsInfoList.get(i).hash).isEqualTo(deserializedNamedRefsInfoList.get(i).hash);

    }
  }

  /**
   *java.lang.NullPointerException
   * 	at org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter.readRefLog(NonTransactionalDatabaseAdapter.java:920)
   * 	at org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter.readRefLog(NonTransactionalDatabaseAdapter.java:130)
   * 	at org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter.readRefLogStream(AbstractDatabaseAdapter.java:1995)
   * 	at org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter.refLog(NonTransactionalDatabaseAdapter.java:1503)
   * 	at ExportNessieRepo.exportRefLogTable(ExportNessieRepo.java:159)
   * 	at TestExportRocks.testRefLogTable(TestExportRocks.java:119) */
  @Test
  public void testRefLogTable()
  {
    String targetDirectory = "/Users/aditya.vemulapalli/Downloads";

    exportNessieRepo.exportRefLogTable(targetDirectory);

    List<RefLog> deserializedRefLog = ExportTestsHelper.deserializeRefLog(targetDirectory);

    List<RefLog> originalReflog = ExportTestsHelper.fetchRefLogList(rocksDatabaseAdapter);

    Assertions.assertThat(originalReflog.size()).isEqualTo(deserializedRefLog.size());

    int j ;

    for(int i = 0 ; i < originalReflog.size(); i++)
    {
      Assertions.assertThat(originalReflog.get(i).getCommitHash()).isEqualTo(deserializedRefLog.get(i).getCommitHash());

      Assertions.assertThat(originalReflog.get(i).getRefLogId()).isEqualTo(deserializedRefLog.get(i).getRefLogId());

      Assertions.assertThat(originalReflog.get(i).getRefName()).isEqualTo(deserializedRefLog.get(i).getRefName());

      Assertions.assertThat(originalReflog.get(i).getRefType()).isEqualTo(deserializedRefLog.get(i).getRefType());

      Assertions.assertThat(originalReflog.get(i).getOperation()).isEqualTo(deserializedRefLog.get(i).getOperation());

      Assertions.assertThat(originalReflog.get(i).getOperationTime()).isEqualTo(deserializedRefLog.get(i).getOperationTime());

      Assert.assertEquals(originalReflog.get(i).getParents(), deserializedRefLog.get(i).getParents());

      Assert.assertEquals(originalReflog.get(i).getSourceHashes(), deserializedRefLog.get(i).getSourceHashes());
    }
  }

  @Test
  public void testCommitLogTable()
  {
    String targetDirectory = "/Users/aditya.vemulapalli/Downloads";

    exportNessieRepo.exportCommitLogTable(targetDirectory);

    List<CommitLogClass1> deserializedCommitLogClass1List = ExportTestsHelper.deserializeCommitLogClass1List(targetDirectory);

    List<CommitLogClass2> deserializedCommitLogClass2List = ExportTestsHelper.deserializeCommitLogClass2List(targetDirectory);

    CommitLogClassWrapper originalCommitLogList = ExportTestsHelper.fetchCommitLogTable(rocksDatabaseAdapter);

    List<CommitLogClass1> commitLogClass1List = originalCommitLogList.commitLogClass1List;
    List<CommitLogClass2> commitLogClass2List = originalCommitLogList.commitLogClass2List;

    Assertions.assertThat(commitLogClass1List.size()).isEqualTo(deserializedCommitLogClass1List.size());

    for(int i = 0 ; i < commitLogClass1List.size(); i++)
    {
      Assertions.assertThat(commitLogClass1List.get(i).commitSeq).isEqualTo(deserializedCommitLogClass1List.get(i).commitSeq);

      Assertions.assertThat(commitLogClass1List.get(i).hash).isEqualTo(deserializedCommitLogClass1List.get(i).hash);

      Assertions.assertThat(commitLogClass1List.get(i).createdTime).isEqualTo(deserializedCommitLogClass1List.get(i).createdTime);

      Assertions.assertThat(commitLogClass1List.get(i).parent_1st).isEqualTo(deserializedCommitLogClass1List.get(i).parent_1st);

      Assert.assertEquals(commitLogClass1List.get(i).additionalParents, deserializedCommitLogClass1List.get(i).additionalParents);

      Assert.assertEquals(commitLogClass1List.get(i).contentIds, deserializedCommitLogClass1List.get(i).contentIds);

      Assert.assertEquals(commitLogClass1List.get(i).deletes, deserializedCommitLogClass1List.get(i).deletes);

      Assert.assertEquals(commitLogClass1List.get(i).noOfStringsInKeys, deserializedCommitLogClass1List.get(i).noOfStringsInKeys);

      Assert.assertEquals(commitLogClass1List.get(i).putsKeyStrings, deserializedCommitLogClass1List.get(i).putsKeyStrings);

      Assert.assertEquals(commitLogClass1List.get(i).putsKeyNoOfStrings, deserializedCommitLogClass1List.get(i).putsKeyNoOfStrings);
    }

    Assertions.assertThat(commitLogClass2List.size()).isEqualTo(deserializedCommitLogClass2List.size());

    for(int i = 0 ; i < commitLogClass2List.size(); i++)
    {
      Assert.assertEquals(commitLogClass2List.get(i).commitMeta, deserializedCommitLogClass2List.get(i).commitMeta);

      Assert.assertEquals(commitLogClass2List.get(i).contents, commitLogClass2List.get(i).contents);
    }

  }

}
