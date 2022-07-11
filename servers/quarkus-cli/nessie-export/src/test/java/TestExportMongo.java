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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.*;
import org.projectnessie.versioned.persist.adapter.*;
import org.projectnessie.versioned.persist.mongodb.ImmutableMongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
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

    refLogTable.close();

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
    List<CommitLogClass2> readCommitLogList2 = new ArrayList<CommitLogClass2>();
    try {
      byte[] data = Files.readAllBytes(pathFile2);
      int noOfBytes = data.length;
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
        readCommitLogList2.add(commitLogClass2);
        i++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Stream<CommitLogEntry> commitLogTable =  mongoDatabaseAdapter.scanAllCommitLogEntries();

    /**entries bounded cache*/
    Map<ContentId, ByteString> globalContents = new HashMap<>();
    Function<KeyWithBytes, ByteString> getGlobalContents =
      (put) ->
        globalContents.computeIfAbsent(
          put.getContentId(),
          cid ->
            mongoDatabaseAdapter
              .globalContent(put.getContentId())
              .map(ContentIdAndBytes::getValue)
              .orElse(null));

    StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
    Serializer<CommitMeta> metaSerializer = storeWorker.getMetadataSerializer();

    List<CommitLogClass1> commitLogList1 = new ArrayList<CommitLogClass1>();
    List<CommitLogClass2> commitLogList2 = new ArrayList<CommitLogClass2>();


    commitLogTable.map(x -> {
      long createdTime = x.getCreatedTime();
      long commitSeq = x.getCommitSeq();
      String hash = x.getHash().asString();

      String parent_1st = x.getParents().get(0).asString();

      List<String> additionalParents = new ArrayList<String>();

      List<Hash> hashAdditionalParents = x.getAdditionalParents();
      for (Hash hashAdditionalParent : hashAdditionalParents) {
        additionalParents.add(hashAdditionalParent.asString());
      }

      List<String> deletes = new ArrayList<String>();
      List<Integer> noOfStringsInKeys = new ArrayList<Integer>();

      List<Key> keyDeletes = x.getDeletes();
      for (Key keyDelete : keyDeletes) {

        List<String> elements = keyDelete.getElements();

        noOfStringsInKeys.add(elements.size());

        deletes.addAll(elements);
      }

      List<KeyWithBytes> puts = x.getPuts();

      ByteString metaDataByteString = x.getMetadata();

      CommitMeta metaData = metaSerializer.fromBytes(metaDataByteString);

      List<String> contentIds = new ArrayList<>();
      List<Content> contents = new ArrayList<>();
      List<String> putsKeyStrings = new ArrayList<>();
      List<Integer> putsKeyNoOfStrings = new ArrayList<>();

      for (KeyWithBytes put : puts) {
        ContentId contentId = put.getContentId();
        contentIds.add(contentId.getId());

        ByteString value = put.getValue();

        Content content = storeWorker.valueFromStore(value, () -> getGlobalContents.apply(put));

        contents.add(content);

        Key key = put.getKey();
        List<String> elements1 = key.getElements();
        putsKeyNoOfStrings.add(elements1.size());
        putsKeyStrings.addAll(elements1);
      }

      commitLogList2.add(new CommitLogClass2(contents, metaData));

      return new CommitLogClass1(createdTime, commitSeq, hash, parent_1st, additionalParents, deletes, noOfStringsInKeys,
        contentIds, putsKeyStrings, putsKeyNoOfStrings);
    }).forEachOrdered(commitLogList1::add);

    commitLogTable.close();

    Assert.assertEquals(readCommitLogList1, commitLogList1);
    Assert.assertEquals(readCommitLogList2, commitLogList2);

  }
}
