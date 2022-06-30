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
import org.projectnessie.versioned.*;
import org.projectnessie.versioned.persist.adapter.*;
import com.google.protobuf.ByteString;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;
import org.projectnessie.versioned.persist.store.PersistVersionStore;


import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.toProto;

public class ExportNessieRepo {

  DatabaseAdapter databaseAdapter;

  StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

  public void getTables( ) throws RefLogNotFoundException, ReferenceNotFoundException, IOException {

    /** handle the exceptions */

    /**************************************************************************************************/
    GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

    Stream<ReferenceInfo<ByteString>> namedReferences = databaseAdapter.namedRefs(params);
    List <ReferenceInfo<ByteString>> namedReferencesList = namedReferences.collect(Collectors.toList());

    String namedRefsTableFilePath = "/Users/aditya.vemulapalli/Downloads/namedRefsTableProto";
    FileOutputStream fosNamedRefs = new FileOutputStream(namedRefsTableFilePath);

    /** message NamedReference { -->only used in Non Tx
      string name = 1;
      RefPointer ref = 2;
    }*/

    /** Write the logic to serialize the named references */
    fosNamedRefs.close();

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

    /** Forgot try " with - resources " to habdle the exception */
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

    for( int i = 0 ; i < refLogList.size(); i++)
    {
      RefLog refLog = refLogList.get(i);

      /** serialize the RefLog */
      /** Should write a function to do serialization of RefLog , common for tx and non tx */
      AdapterTypes.RefLogEntry refLogEntry ;
      /** refLogEntry.writeTo(fosRefLog); */
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
    List<Hash> sourceHashes = refLog.getSourceHashes();
    List<Hash> parents = refLog.getParents();
    /** set operation, parents , source hashes are need to be initiaized */

    AdapterTypes.RefLogEntry.Builder proto =
      AdapterTypes.RefLogEntry.newBuilder()
        .setRefLogId(refLog.getRefLogId().asBytes())
        .setRefName(ByteString.copyFromUtf8(refLog.getRefName()))
        .setRefType(refType)
        .setCommitHash(refLog.getCommitHash().asBytes())
        .setOperationTime(refLog.getOperationTime())
        .setOperation();
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
