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
import com.google.gson.GsonBuilder;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.*;
import org.projectnessie.versioned.persist.adapter.*;
import com.google.protobuf.ByteString;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;
import org.projectnessie.versioned.persist.store.PersistVersionStore;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.toProto;

public class export2 {

  DatabaseAdapter databaseAdapter;

  StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
  public export2() throws FileNotFoundException {
  }

  public void getTables( ) throws RefLogNotFoundException, ReferenceNotFoundException, IOException {

    /** handle the exceptions */

    /**************************************************************************************************/
    GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

    Stream<ReferenceInfo<ByteString>> namedReferences = databaseAdapter.namedRefs(params);
    List <ReferenceInfo<ByteString>> namedReferencesList = namedReferences.collect(Collectors.toList());

    String namedRefsTableFilePath = "/Users/aditya.vemulapalli/Downloads/namedRefsTableProto";
    // FileOutputStream fosNamedRefs = new FileOutputStream(namedRefsTableFilePath);
    Writer writer = new FileWriter(namedRefsTableFilePath);

    /**Using GSON for serialization and de - serialization*/
    /** Write the logic to serialize the named references */
    /** Serialization must be such that , deserialization must be easy */
    for (ReferenceInfo<ByteString> namedref : namedReferencesList) {
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      gson.toJson(namedref, writer);
    }
    writer.close();
    // fosNamedRefs.close();

    /**************************************************************************************************/

    Stream<CommitLogEntry> commitLogTable =  databaseAdapter.scanAllCommitLogEntries();
    List<CommitLogEntry> commitLogList = commitLogTable.collect(Collectors.toList());

    String commitLogTableFilePath = "/Users/aditya.vemulapalli/Downloads/commitLogTableProto";
    FileOutputStream fosCommitLog = new FileOutputStream(commitLogTableFilePath);

    /** Write the logic to serialize Commit Log Entries */
    fosCommitLog.close();

    /**************************************************************************************************/


    RepoDescription repoDescTable = databaseAdapter.fetchRepositoryDescription();
    // Serializing Repository description
    AdapterTypes.RepoProps repoProps = toProto(repoDescTable);
    // protoc -I=. --java_out=. persist.proto
    String repoDescFilePath = "/Users/aditya.vemulapalli/Downloads/repoDescProto";

    /** Forgot try " with - resources " to handle the exception */
    FileOutputStream fosDescTable = new FileOutputStream(repoDescFilePath);
    repoProps.writeTo(fosDescTable);
    fosDescTable.close();

    /**************************************************************************************************/

    /** test null works or not */
    Stream<RefLog> refLogTable = databaseAdapter.refLog(null);
    /** Will the list be in the same order of stream ( is Stream an actual order of RefLogTable )  */
    List<RefLog> refLogList = refLogTable.collect(Collectors.toList());

    String refLogTableFilePath = "/Users/aditya.vemulapalli/Downloads/refLogTableProto";
    FileOutputStream fosRefLog = new FileOutputStream(refLogTableFilePath);

    /** serialize the RefLog */
    /** Should write a function to do serialization of RefLog , common for tx and non tx */
    for (RefLog refLog : refLogList) {
      /** serialize the RefLog */
      /** Should write a function to do serialization of RefLog , common for tx and non tx */
      AdapterTypes.RefLogEntry refLogEntry = toProtoFromRefLog(refLog);
      refLogEntry.writeTo(fosRefLog);
    }
    fosRefLog.close();

    /**************************************************************************************************/
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

    AdapterTypes.RefLogEntry refLogEntry = proto.build();
    return refLogEntry;
  }

//  ContentId contentId;
//
//  Optional<ContentIdAndBytes> globalContent = databaseAdapter.globalContent(contentId);
//
//  ByteString onReferenceValue;
//
//  Supplier<ByteString> globalState ;
//
//  CONTENT content1 = storeWorker.valueFromStore(onReferenceValue, globalState );
//
//  Content content;
//
//  VersionStore<Content, CommitMeta, Content.Type> versionStore =
//    new PersistVersionStore<>(databaseAdapter, storeWorker);

}
