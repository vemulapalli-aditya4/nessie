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
import java.util.List;

public class CommitLogClass1 implements Serializable {

    public long createdTime;
    public long commitSeq;

    public String hash;

    public String parent_1st;

    public List<String> additionalParents;

    // public List<Key> deletes; where key has a necessary part of List of string, so List<key> = List<List<String>>
    //We collapse this into a list of String and store number of Strings in each key
    public List<String> deletes;

    public List<Integer> noOfStringsInKeys;

    public List<String> contentIds;

    public List<String> putsKeyStrings;

    public List<Integer> putsKeyNoOfStrings;

    public CommitLogClass1(long createdTime, long commitSeq, String hash, String parent_1st,
                           List<String> additionalParents, List<String > deletes, List<Integer> noOfStringsInKeys,
                           List<String> contentIds ,
                           List<String> putsKeyStrings, List<Integer> putsKeyNoOfStrings) {
        this.commitSeq = commitSeq;
        this.createdTime = createdTime;
        this.hash = hash;
        this.parent_1st = parent_1st;
        this.additionalParents = additionalParents;
        this.deletes = deletes;
        this.noOfStringsInKeys = noOfStringsInKeys;
        this.contentIds = contentIds;
        this.putsKeyStrings = putsKeyStrings;
        this.putsKeyNoOfStrings = putsKeyNoOfStrings;
    }

}
