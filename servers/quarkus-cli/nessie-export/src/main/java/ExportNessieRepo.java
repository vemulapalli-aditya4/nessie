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
import org.projectnessie.versioned.persist.adapter.*;
import com.google.protobuf.ByteString;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.StoreWorker;


import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ExportNessieRepo {

  Content content;

  DatabaseAdapter databaseAdapter;

  StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

  VersionStore<Content, CommitMeta, Content.Type> versionStore =
    new PersistVersionStore<>(databaseAdapter, storeWorker);


  GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

  Stream<ReferenceInfo<ByteString>> namedReferences = databaseAdapter.namedRefs(params);

  Stream<CommitLogEntry> commitLogTable =  databaseAdapter.scanAllCommitLogEntries();

  RepoDescription repoDescTable = databaseAdapter.fetchRepositoryDescription();

  Stream<RefLog> refLogTable = databaseAdapter.refLog(null);

  ContentId contentId;

  Optional<ContentIdAndBytes> globalContent = databaseAdapter.globalContent(contentId);

  ByteString onReferenceValue;

  Supplier<ByteString> globalState ;

  CONTENT content1 = storeWorker.valueFromStore(onReferenceValue, globalState );


}
