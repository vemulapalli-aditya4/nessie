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


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.toProto;

public class ExportNessieRepo {

  DatabaseAdapter databaseAdapter;

  StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
  public ExportNessieRepo() throws FileNotFoundException {
  }

  public void getTables( ) throws RefLogNotFoundException, ReferenceNotFoundException, IOException {

    /** handle the exceptions */

    /**************************************************************************************************/
    GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

    Stream<ReferenceInfo<ByteString>> namedReferences = databaseAdapter.namedRefs(params);
    List <ReferenceInfo<ByteString>> namedReferencesList = namedReferences.collect(Collectors.toList());

    String namedRefsTableFilePath = "/Users/aditya.vemulapalli/Downloads/namedRefs.json";
    Writer writer = new FileWriter(namedRefsTableFilePath);

    /**Using GSON for serialization and de - serialization*/
    /** Serialization is straight forward , deserialization must be done using custom deserializer */

    /**Gson gson = new GsonBuilder().create(); --->for non readable format */

    Gson gson = new Gson();
    gson.toJson(namedReferencesList, writer);

    writer.close();

    /**************************************************************************************************/

    Stream<CommitLogEntry> commitLogTable =  databaseAdapter.scanAllCommitLogEntries();
    List<CommitLogEntry> commitLogList = commitLogTable.collect(Collectors.toList());

    /**To store the created time, commit seq, Hash , 1st parent*/
    String commitLogTableFilePath1 = "/Users/aditya.vemulapalli/Downloads/commitLogFile1.json";
    Writer writerCommitLogFile1 = new FileWriter(commitLogTableFilePath1);
    Gson gsonCommitLogfile1 = new Gson();
    List<Class1ForCommitLog> commitLog1 = new ArrayList<Class1ForCommitLog>();

    /** To store the additional Parents*/
    String commitLogTableFilePath2 = "/Users/aditya.vemulapalli/Downloads/commitLogFile2.json";
    Writer writerCommitLogFile2 = new FileWriter(commitLogTableFilePath2);
    Gson gsonCommitLogfile2 = new Gson();
    List<String> commitLog2 = new ArrayList<String>();

    /** To store the counts of additional Parents of each commitLogEntry */
    String commitLogTableFilePath3 = "/Users/aditya.vemulapalli/Downloads/commitLogFile3.json";
    Writer writerCommitLogFile3 = new FileWriter(commitLogTableFilePath3);
    Gson gsonCommitLogfile3 = new Gson();
    List<Integer> commitLog3 = new ArrayList<Integer>();

    List<KeyWithBytes> puts;
    List<Key> deletes;
    ByteString metaData1;

    for( CommitLogEntry commitLogEntry : commitLogList)
    {
      Class1ForCommitLog var1 = new Class1ForCommitLog(commitLogEntry.getCreatedTime(),
        commitLogEntry.getCommitSeq(),
        commitLogEntry.getHash().asString(),
        commitLogEntry.getParents().get(0).asString());

      commitLog1.add(var1);

      List<Hash> additionalParents = commitLogEntry.getAdditionalParents();
      if(additionalParents == null)
      {
        commitLog3.add(-1);
      }
      else{
        commitLog3.add(additionalParents.size());
        for (Hash additionalParent : additionalParents) {
          commitLog2.add(additionalParent.asString());
        }
      }

      /**Ask whether this is the metadata meant or any other form  */
      ByteString metaData = commitLogEntry.getMetadata();

    }

    gsonCommitLogfile1.toJson(commitLog1, writerCommitLogFile1);
    writerCommitLogFile1.close();

    gsonCommitLogfile2.toJson(commitLog2, writerCommitLogFile2);
    writerCommitLogFile2.close();

    gsonCommitLogfile3.toJson(commitLog3, writerCommitLogFile3);
    writerCommitLogFile3.close();

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
