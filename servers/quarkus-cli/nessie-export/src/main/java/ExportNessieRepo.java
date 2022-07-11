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
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.*;
import org.projectnessie.versioned.persist.adapter.*;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.*;

public class ExportNessieRepo {

  DatabaseAdapter databaseAdapter;

  StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

  /** Actually should take export directory location string as parameter */
  /**String targetDirectory*/
  public void exportRepoDesc()
  {
    /** Right now there is no use for Repository Description table
     * The exported file will just be an empty file */

    RepoDescription repoDescTable = databaseAdapter.fetchRepositoryDescription();

    AdapterTypes.RepoProps repoProps = toProto(repoDescTable);

    /**String repoDescFilePath = targetDirectory + "/repoDesc"*/
    String repoDescFilePath = "/Users/aditya.vemulapalli/Downloads/repoDesc";

    byte[] arr = repoProps.toByteArray();
    FileOutputStream fosDescTable = null;

    try{
      fosDescTable = new FileOutputStream(repoDescFilePath);
      fosDescTable.write(arr);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if(fosDescTable != null)
      {
        try {
          fosDescTable.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    /** Deserialization Logic*/

    /**Path path = Paths.get(targetDirectory + "/repoDesc" )*/
    Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/repoDesc");
    try {
      byte[] data = Files.readAllBytes(path);
      RepoDescription repoDesc = protoToRepoDescription(data);
      System.out.println("Repo version is " + repoDesc.getRepoVersion());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  /** Actually should take export directory location string as parameter */
  /**String targetDirectory*/
  public void exportNamedRefs()
  {
    GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

    /**String namedRefsFilePath = targetDirectory + "/namedRefs"*/

    String namedRefsFilePath = "/Users/aditya.vemulapalli/Downloads/namedRefs";

    List<ReferenceInfoExport> namedRefsInfoList;
    namedRefsInfoList = new ArrayList<ReferenceInfoExport>();
    Stream<ReferenceInfo<ByteString>> namedReferences = null;
    FileOutputStream fileOut = null;
    ObjectOutputStream out = null;

    try {
      namedReferences = databaseAdapter.namedRefs(params);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    try {
      fileOut = new FileOutputStream(namedRefsFilePath);
      out = new ObjectOutputStream(fileOut);

      namedReferences.map(x -> {
        String referenceName = x.getNamedRef().getName();

        String type  = " "; /** must get this */
        if(x.getNamedRef() instanceof ImmutableBranchName)
        {
          type = "branch";
        } else if (x.getNamedRef() instanceof ImmutableTagName) {
          type = "tag";
        }

        String hash = x.getHash().asString();

        return  new ReferenceInfoExport(referenceName, type, hash);
      }).forEach(namedRefsInfoList::add);


      out.writeObject(namedRefsInfoList);
      out.close();
      fileOut.close();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    /** Deserialization Logic*/
    FileInputStream fileIn = null;
    ObjectInputStream in = null;
    List<ReferenceInfoExport> readNamedRefsInfoList = new ArrayList<ReferenceInfoExport>();
    try{
      fileIn = new FileInputStream(namedRefsFilePath);
      in = new ObjectInputStream(fileIn);

      readNamedRefsInfoList = (ArrayList) in.readObject();
      in.close();
      fileIn.close();

      for (ReferenceInfoExport referenceInfoExport : readNamedRefsInfoList) {
        System.out.println("" + referenceInfoExport.referenceName);
        System.out.println("" + referenceInfoExport.type);
        System.out.println("" + referenceInfoExport.hash);
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

  }

  /** Actually should take export directory location string as parameter */
  /**String targetDirectory*/
  public void exportRefLogTable()
  {

    Stream<RefLog> refLogTable = null;
    try {
      refLogTable = databaseAdapter.refLog(null);
    } catch (RefLogNotFoundException e) {
      throw new RuntimeException(e);
    }

    /**String namedRefsFilePath = targetDirectory + "/refLogTable"*/
    String refLogTableFilePath = "/Users/aditya.vemulapalli/Downloads/refLogTable";

    FileOutputStream fosRefLog = null;
    try{

      fosRefLog = new FileOutputStream(refLogTableFilePath);
      FileOutputStream finalFosRefLog = fosRefLog;
      refLogTable.map(x-> {
        AdapterTypes.RefLogEntry refLogEntry = toProtoFromRefLog(x);
        return refLogEntry.toByteArray();
      }).forEachOrdered(y ->{

        int len = y.length;
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(len);
        byte[] bytes = bb.array();

        try {
          finalFosRefLog.write(bytes);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        try {
          finalFosRefLog.write(y);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });

      fosRefLog.close();

      finalFosRefLog.close();
      refLogTable.close();

      System.out.println("Ref Log Table is written");

    } catch(IOException e ) {
      throw new RuntimeException(e);
    }

    /** Deserialization Logic*/
    Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/refLogTable");
    try {
      byte[] data = Files.readAllBytes(path);
      int noOfBytes = data.length;
      List<RefLog> deserializedRefLogTable = new ArrayList<RefLog>();
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

  }

  public void exportCommitLogTable()
  {

    String commitLogTableFilePath1 = "/Users/aditya.vemulapalli/Downloads/commitLogFile1";
    String commitLogTableFilePath2 = "/Users/aditya.vemulapalli/Downloads/commitLogFile2";

    Stream<CommitLogEntry> commitLogTable =  databaseAdapter.scanAllCommitLogEntries();

    /**entries bounded cache*/
    Map<ContentId, ByteString> globalContents = new HashMap<>();
    Function<KeyWithBytes, ByteString> getGlobalContents =
      (put) ->
        globalContents.computeIfAbsent(
          put.getContentId(),
          cid ->
            databaseAdapter
              .globalContent(put.getContentId())
              .map(ContentIdAndBytes::getValue)
              .orElse(null));

    Serializer<CommitMeta> metaSerializer = storeWorker.getMetadataSerializer();

    List<CommitLogClass1> commitLogList1 = new ArrayList<CommitLogClass1>();
    List<CommitLogClass2> commitLogList2 = new ArrayList<CommitLogClass2>();

    FileOutputStream fileOut1 = null;
    ObjectOutputStream out1 = null;
    FileOutputStream fileOut2 = null;
    try{
      fileOut1 = new FileOutputStream(commitLogTableFilePath1);
      out1 = new ObjectOutputStream(fileOut1);
      fileOut2 = new FileOutputStream(commitLogTableFilePath2);

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

        List <String> contentIds = new ArrayList<>();
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

      for (CommitLogClass2 commitLogClass2 : commitLogList2) {

        byte[] arr ;
        ObjectMapper objectMapper = new ObjectMapper();

        try{
          arr = objectMapper.writeValueAsBytes(commitLogClass2);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }

        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(arr.length);
        byte[] bytes = bb.array();
        try{
          fileOut2.write(bytes);
          fileOut2.write(arr);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      out1.writeObject(commitLogList1);
      out1.close();
      fileOut1.close();
      fileOut2.close();
      commitLogTable.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    /** Deserialization Logic*/

    //For file 1
    FileInputStream fileIn = null;
    ObjectInputStream in = null;
    List<CommitLogClass1> readCommitLogList1 = new ArrayList<CommitLogClass1>();
    try{
      fileIn = new FileInputStream(commitLogTableFilePath1);
      in = new ObjectInputStream(fileIn);

      readCommitLogList1 = (ArrayList) in.readObject();
      in.close();
      fileIn.close();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    //For file2
    Path pathFile2 = Paths.get("/Users/aditya.vemulapalli/Downloads/commitLogFile2" );
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
        i++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public AdapterTypes.RefLogEntry toProtoFromRefLog(RefLog refLog)
  {
    /** Reference type can be 'Branch' or 'Tag'. */
    AdapterTypes.RefType refType = Objects.equals(refLog.getRefType(), "Tag") ? AdapterTypes.RefType.Tag : AdapterTypes.RefType.Branch;
    /**enum Operation { __>RefLogEntry persist.proto
     CREATE_REFERENCE = 0;
     COMMIT = 1;
     DELETE_REFERENCE = 2;
     ASSIGN_REFERENCE = 3;
     MERGE = 4;
     TRANSPLANT = 5;
     }*/

    String op = refLog.getOperation();
    AdapterTypes.RefLogEntry.Operation operation = AdapterTypes.RefLogEntry.Operation.TRANSPLANT;

    /** Confirm whether the string ops are correct or not */
    if(Objects.equals(op, "CREATE_REFERENCE"))
    {
      operation = AdapterTypes.RefLogEntry.Operation.CREATE_REFERENCE;
    } else if (Objects.equals(op, "COMMIT")) {
      operation = AdapterTypes.RefLogEntry.Operation.COMMIT;
    } else if ( Objects.equals(op, "DELETE_REFERENCE") ) {
      operation = AdapterTypes.RefLogEntry.Operation.DELETE_REFERENCE;
    } else if (Objects.equals(op, "ASSIGN_REFERENCE") ) {
      operation = AdapterTypes.RefLogEntry.Operation.ASSIGN_REFERENCE;
    } else if (Objects.equals(op, "MERGE")) {
      operation = AdapterTypes.RefLogEntry.Operation.MERGE;
    }

    AdapterTypes.RefLogEntry.Builder proto =
      AdapterTypes.RefLogEntry.newBuilder()
        .setRefLogId(refLog.getRefLogId().asBytes())
        .setRefName(ByteString.copyFromUtf8(refLog.getRefName()))
        .setRefType(refType)
        .setCommitHash(refLog.getCommitHash().asBytes())
        .setOperationTime(refLog.getOperationTime())
        .setOperation(operation);

    List<Hash> sourceHashes = refLog.getSourceHashes();
    sourceHashes.forEach(hash -> proto.addSourceHashes(hash.asBytes()));

    Stream<ByteString> parents = refLog.getParents().stream().map(Hash::asBytes);
    parents.forEach(proto::addParents);

    return proto.build();
  }
}
