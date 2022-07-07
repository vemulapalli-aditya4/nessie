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
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

public class CommitMetaInfo implements Serializable {

  public String author;

  public Instant commitTime;

  public Instant authorTime;

  public String hash;

  public String committer;

  public String message;

  public Map<String, String> properties;

  public String signedOffBy;

  public CommitMetaInfo(String author, Instant commitTime, Instant authorTime,
                        String hash, String committer, String message,
                        Map<String, String> properties, String signedOffBy)
  {
    this.author = author;
    this.commitTime = commitTime;
    this.authorTime = authorTime;
    this.hash = hash;
    this.committer = committer;
    this.message = message;
    this.properties = properties;
    this.signedOffBy = signedOffBy;
  }

}
