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

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.mongodb.ImmutableMongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

public class TestExportMongo {

  DatabaseAdapter mongoDatabaseAdapter;

  ExportNessieRepo exportNessieRepo;

  public TestExportMongo()
  {
    MongoClientConfig mongoClientConfig = ImmutableMongoClientConfig.builder()
      .connectionString("mongodb://root:password@localhost:27017").databaseName("nessie").build();

    MongoDatabaseClient MongoDBClient = new MongoDatabaseClient();
    MongoDBClient.configure(mongoClientConfig);
    MongoDBClient.initialize();

    StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

    mongoDatabaseAdapter = new MongoDatabaseAdapterFactory()
      .newBuilder()
      .withConnector(MongoDBClient)
      .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
      .build(storeWorker);

    exportNessieRepo = new ExportNessieRepo(mongoDatabaseAdapter);
  }

  public void TestRepoDesc() {



  }


}
