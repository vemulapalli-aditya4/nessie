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

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.*;
import org.projectnessie.versioned.persist.adapter.*;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRepoDescription;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.toProto;

public class ExportNessieRepoNew1  {

  DatabaseAdapter databaseAdapter;

  StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
  public ExportNessieRepoNew1() throws FileNotFoundException {
  }

  /** Actually should take export directory location string as parameter */
  public void exportNamedRefs()
  {
    /**Do the params should be DEFAULT ??*/
    GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

    Stream<ReferenceInfo<ByteString>> namedReferences = null;
    try {
      namedReferences = databaseAdapter.namedRefs(params);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
    List<ReferenceInfo<ByteString>> namedReferencesList = namedReferences.collect(Collectors.toList());

    String namedRefsTableFilePath = "/Users/aditya.vemulapalli/Downloads/namedRefs.json";
    Writer writer = null;
    Gson gson = new Gson();

    /**Using GSON for serialization and de - serialization*/
    /** Serialization is straight forward , deserialization must be done using custom deserializer */

    try{
      writer = new FileWriter(namedRefsTableFilePath);
      gson.toJson(namedReferencesList, writer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if(writer != null)
      {
        try {
          writer.close();
        }
        catch(IOException e){
          e.printStackTrace();
        }
      }
    }
  }

  /** Actually should take export directory location string as parameter */
  public void exportRepoDesc()
  {
    RepoDescription repoDescTable = databaseAdapter.fetchRepositoryDescription();
    // Serializing Repository description
    AdapterTypes.RepoProps repoProps = toProto(repoDescTable);
    // protoc -I=. --java_out=. persist.proto
    String repoDescFilePath = "/Users/aditya.vemulapalli/Downloads/repoDescProto";

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
//    /** Deserialization Logic*/
//    Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/repoDescProto");
//    try {
//      byte[] data = Files.readAllBytes(path);
//      RepoDescription repoDesc = protoToRepoDescription(data);
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }

  }

  /** Actually should take export directory location string as parameter */
  public void exportRefLogTable()
  {
    Stream<RefLog> refLogTable = null;
    try {
      refLogTable = databaseAdapter.refLog(null);
    } catch (RefLogNotFoundException e) {
      throw new RuntimeException(e);
    }
    /** Will the list be in the same order of stream ( is Stream an actual order of RefLogTable )  */
    List<RefLog> refLogList = refLogTable.collect(Collectors.toList());

    String refLogTableFilePath = "/Users/aditya.vemulapalli/Downloads/refLogTableProto";
    List<AdapterTypes.RefLogEntry> refLogEntries = new ArrayList<AdapterTypes.RefLogEntry>();

    for (RefLog refLog : refLogList) {
      AdapterTypes.RefLogEntry refLogEntry = toProtoFromRefLog(refLog);
      refLogEntries.add(refLogEntry);
    }

    FileOutputStream fosRefLog = null;
    try{
      fosRefLog = new FileOutputStream(refLogTableFilePath);
      for (AdapterTypes.RefLogEntry refLogEntry : refLogEntries) {

        byte[] refLogEntryByteArray = refLogEntry.toByteArray();
        int len = refLogEntryByteArray.length;
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(len);
        byte[] bytes = bb.array();
        fosRefLog.write(bytes);
        fosRefLog.write(refLogEntryByteArray);
      }
    } catch( FileNotFoundException e ) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if(fosRefLog != null)
      {
        try{
          fosRefLog.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    }
  }

  /** Actually should take export directory location string as parameter */
  public void exportCommitLogTable()
  {
    Stream<CommitLogEntry> commitLogTable =  databaseAdapter.scanAllCommitLogEntries();
    List<CommitLogEntry> commitLogList = commitLogTable.collect(Collectors.toList());
    List<AdapterTypes.CommitLogEntry> commitLogEntries = new ArrayList<AdapterTypes.CommitLogEntry>();
    String commitLogTableFilePath = "/Users/aditya.vemulapalli/Downloads/commitLogFile";

    Content content;

    ContentId contentId;

    ByteString value;

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

    for( CommitLogEntry commitLogEntry : commitLogList)
    {
      AdapterTypes.CommitLogEntry protoOriginal = toProto(commitLogEntry);
      List<KeyWithBytes> puts = commitLogEntry.getPuts();

      /** Array List ??*/
      List<KeyWithBytes> newPuts  = new ArrayList<>();
      for (KeyWithBytes put : puts) {

        /** Modify the value ?? */
        value = put.getValue();
        contentId = put.getContentId();
        content = storeWorker.valueFromStore(value, () -> getGlobalContents.apply(put) ) ;

        KeyWithBytes newPut = KeyWithBytes.of( put.getKey(), contentId , put.getType() , value);
        newPuts.add(newPut);
      }

      AdapterTypes.CommitLogEntry protoModified = AdapterTypes.CommitLogEntry.newBuilder()
        .mergeFrom(protoOriginal)
        .clearParents()
        .addParents(commitLogEntry.getParents().get(0).asBytes())
        .clearKeyListDistance()
        .clearKeyListIds()
        .clearKeyList()
        .clearPuts()
        // .addAllPuts(newPuts)
        /** Must use .addAllPuts() to add new puts */
        .build();

      commitLogEntries.add(protoModified);
    }

    FileOutputStream fosCommitLog = null;
    try{
      fosCommitLog = new FileOutputStream(commitLogTableFilePath);
      for(AdapterTypes.CommitLogEntry commitLogEntry : commitLogEntries)
      {
        byte[] arr = commitLogEntry.toByteArray();
        int len = arr.length;
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(len);
        byte[] bytes = bb.array();
        fosCommitLog.write(bytes);
        fosCommitLog.write(arr);

      }
    } catch( FileNotFoundException e ) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if(fosCommitLog != null)
      {
        try{
          fosCommitLog.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    }
  }
  public void getTables( ){

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
      operation = AdapterTypes.RefLogEntry.Operation.COMMIT;
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
