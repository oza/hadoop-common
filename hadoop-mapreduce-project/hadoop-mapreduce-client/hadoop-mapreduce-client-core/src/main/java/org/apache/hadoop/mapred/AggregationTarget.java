/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * A class that represents the communication between the tasktracker and child
 * tasks w.r.t the map task completion events. It also indicates whether the
 * child task should reset its events index.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AggregationTarget implements Writable {
  TaskAttemptID[] targets;

  public AggregationTarget() { }

  public AggregationTarget(TaskAttemptID[] targets) {
    this.targets = targets;
  }

  public TaskAttemptID[] getAggregationTargets() {
    return targets;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(targets.length);
    for (TaskAttemptID target : targets) {
      target.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    targets = new TaskAttemptID[in.readInt()];
    for (int i = 0; i < targets.length; ++i) {
      targets[i] = new TaskAttemptID();
      targets[i].readFields(in);
    }
  }
}
