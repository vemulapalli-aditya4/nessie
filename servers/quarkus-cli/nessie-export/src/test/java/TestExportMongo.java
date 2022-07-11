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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.*;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.mongodb.ImmutableMongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRefLog;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRepoDescription;

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

  @Test
  public void testRepoDesc() {

    String targetDirectory = "/Users/aditya.vemulapalli/Downloads";
    exportNessieRepo.exportRepoDesc(targetDirectory);

    /**Testing the serialized repo desc is correct or not */
    /**The repo desc file must be empty */

    /** Deserialization Logic*/
    Path path = Paths.get(targetDirectory + "/repoDesc" );
    RepoDescription repoDesc;
    try {
      byte[] data = Files.readAllBytes(path);
      int len = data.length;
      Assertions.assertThat(len).isEqualTo(0);
      repoDesc = protoToRepoDescription(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testNamedRefs(){
    String targetDirectory = "/Users/aditya.vemulapalli/Downloads";

    exportNessieRepo.exportNamedRefs(targetDirectory);

    List<ReferenceInfoExport> namedRefsInfoList;
    namedRefsInfoList = new ArrayList<ReferenceInfoExport>();
    Stream<ReferenceInfo<ByteString>> namedReferences = null;
    GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

    try {
      namedReferences = mongoDatabaseAdapter.namedRefs(params);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    namedReferences.map(x -> {
      String referenceName = x.getNamedRef().getName();

      String type = " "; /** must get this */
      if (x.getNamedRef() instanceof ImmutableBranchName) {
        type = "branch";
      } else if (x.getNamedRef() instanceof ImmutableTagName) {
        type = "tag";
      }

      String hash = x.getHash().asString();

      return new ReferenceInfoExport(referenceName, type, hash);
    }).forEach(namedRefsInfoList::add);


    /** Deserialization Logic*/
    FileInputStream fileIn = null;
    ObjectInputStream in = null;
    List<ReferenceInfoExport> deserializedNamedRefsInfoList = new ArrayList<ReferenceInfoExport>();
    try{
      fileIn = new FileInputStream(targetDirectory + "/namedRefs");
      in = new ObjectInputStream(fileIn);

      deserializedNamedRefsInfoList = (ArrayList) in.readObject();
      in.close();
      fileIn.close();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    Assert.assertEquals(deserializedNamedRefsInfoList, namedRefsInfoList);
  }

  @Test
  public void testRefLogTable()
  {

    String targetDirectory = "/Users/aditya.vemulapalli/Downloads";

    exportNessieRepo.exportRefLogTable(targetDirectory);

    /** Deserialization Logic*/
    List<RefLog> deserializedRefLogTable = new ArrayList<RefLog>();
    Path path = Paths.get(targetDirectory + "/refLogTable");
    try {
      byte[] data = Files.readAllBytes(path);
      int noOfBytes = data.length;
      // ByteBuffer byteBuffer = ByteBuffer.wrap(data);
      int from = 0 ;
      int size;
      byte[] sizeArr;
      byte[] obj;
      while(noOfBytes != 0)
      {
        sizeArr = Arrays.copyOfRange(data, from, from + 4);
        size = new BigInteger(sizeArr).intValue();
        from += 4;
        noOfBytes -= 4;
        obj = Arrays.copyOfRange(data, from , from + size );
        from += size;
        noOfBytes -= size;
        deserializedRefLogTable.add(protoToRefLog(obj));
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Stream<RefLog> refLogTable = null;
    try {
      refLogTable = mongoDatabaseAdapter.refLog(null);
    } catch (RefLogNotFoundException e) {
      throw new RuntimeException(e);
    }

    List<RefLog> refLogList = refLogTable.collect(Collectors.toList());

    Assert.assertEquals(deserializedRefLogTable, refLogList);
  }

  @Test
  public void testCommitLogTable()
  {
    String targetDirectory = "/Users/aditya.vemulapalli/Downloads";

    exportNessieRepo.exportCommitLogTable(targetDirectory);

    /** Deserialization Logic*/

    //For file 1
    FileInputStream fileIn = null;
    ObjectInputStream in = null;
    List<CommitLogClass1> readCommitLogList1 = new ArrayList<CommitLogClass1>();
    try{
      fileIn = new FileInputStream(targetDirectory + "/commitLogFile1");
      in = new ObjectInputStream(fileIn);

      readCommitLogList1 = (ArrayList) in.readObject();
      in.close();
      fileIn.close();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    //For file2
    Path pathFile2 = Paths.get(targetDirectory + "/commitLogFile2");
    try {
      byte[] data = Files.readAllBytes(pathFile2);
      int noOfBytes = data.length;
      List<CommitLogClass2> deserializedRefLogTable = new ArrayList<CommitLogClass2>();
      int from = 0 ;
      int size;
      byte[] sizeArr;
      byte[] obj;
      int i = 0;
      while(noOfBytes != 0)
      {
        sizeArr = Arrays.copyOfRange(data, from, from + 4);
        size = new BigInteger(sizeArr).intValue();
        from += 4;
        noOfBytes -= 4;
        obj = Arrays.copyOfRange(data, from , from + size );
        from += size;
        noOfBytes -= size;
        ObjectMapper objectMapper = new ObjectMapper();
        CommitLogClass2 commitLogClass2 = objectMapper.readValue(obj, CommitLogClass2.class);
        deserializedRefLogTable.add(commitLogClass2);
        i++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
