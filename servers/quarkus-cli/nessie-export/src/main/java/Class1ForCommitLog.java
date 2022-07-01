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

import org.projectnessie.versioned.Hash;

import java.io.Serializable;

public class Class1ForCommitLog implements Serializable {
  public long createdTime;
  public long commitSeq;

  public String hash;

  public String parent_1st;

  public Class1ForCommitLog(long createdTime, long commitSeq, String hash, String parent_1st) {
    this.commitSeq = commitSeq;
    this.createdTime = createdTime;
    this.hash = hash;
    this.parent_1st = parent_1st;

  }
}
