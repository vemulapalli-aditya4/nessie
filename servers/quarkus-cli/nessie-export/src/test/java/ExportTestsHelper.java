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
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.*;
import org.projectnessie.versioned.persist.adapter.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRefLog;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRepoDescription;

public final class ExportTestsHelper {

  private ExportTestsHelper() {}

  public static List<RefLog> deserializeRefLog(String targetDirectory)
  {
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

    return  deserializedRefLogTable;
  }

  public static List<RefLog> fetchRefLogList(DatabaseAdapter databaseAdapter)
  {
    Stream<RefLog> refLogTable = null;
    try {
      refLogTable = databaseAdapter.refLog(null);
    } catch (RefLogNotFoundException e) {
      throw new RuntimeException(e);
    }

    List<RefLog> refLogList = new ArrayList<RefLog>();
    refLogTable.forEachOrdered(refLogList::add);

    refLogTable.close();

    return refLogList;
  }

  public static List<ReferenceInfoExport> deserializeNamedRefsInfoList(String targetDirectory)
  {
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

    return deserializedNamedRefsInfoList;
  }

  public static List<ReferenceInfoExport> fetchNamedRefsInfoList(DatabaseAdapter databaseAdapter)
  {
    List<ReferenceInfoExport> namedRefsInfoList;
    namedRefsInfoList = new ArrayList<ReferenceInfoExport>();
    Stream<ReferenceInfo<ByteString>> namedReferences = null;
    GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

    try {
      namedReferences = databaseAdapter.namedRefs(params);
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
    }).forEachOrdered(namedRefsInfoList::add);

    return namedRefsInfoList;

  }

  public static int fetchBytesInRepoDesc(String targetDirectory){

    /** Deserialization Logic*/
    Path path = Paths.get(targetDirectory + "/repoDesc" );
    RepoDescription repoDesc;
    int len = -1;
    try {
      byte[] data = Files.readAllBytes(path);
      len = data.length;
      repoDesc = protoToRepoDescription(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return len;
  }

  public static List<CommitLogClass1> deserializeCommitLogClass1List(String targetDirectory){
    /** Deserialization Logic*/
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

    return readCommitLogList1;
  }

  public static List<CommitLogClass2> deserializeCommitLogClass2List(String targetDirectory)
  {
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
    return readCommitLogList2;
  }

  public static CommitLogClassWrapper fetchCommitLogTable(DatabaseAdapter databaseAdapter)
  {
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

    StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
    Serializer<CommitMeta> metaSerializer = storeWorker.getMetadataSerializer();

    List<CommitLogClass1> commitLogClass1List = new ArrayList<CommitLogClass1>();
    List<CommitLogClass2> commitLogClass2List = new ArrayList<CommitLogClass2>();


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

      commitLogClass2List.add(new CommitLogClass2(contents, metaData));

      return new CommitLogClass1(createdTime, commitSeq, hash, parent_1st, additionalParents, deletes, noOfStringsInKeys,
        contentIds, putsKeyStrings, putsKeyNoOfStrings);
    }).forEachOrdered(commitLogClass1List::add);

    commitLogTable.close();

    return new CommitLogClassWrapper(commitLogClass1List, commitLogClass2List);
  }
}
