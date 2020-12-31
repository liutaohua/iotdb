/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.storagegroup.virtualSg;

import org.apache.iotdb.db.metadata.PartialPath;

public interface VirtualPartitioner {

  /**
   * use device id to determine storage group id
   *
   * @param deviceId device id
   * @return virtual storage group id
   */
  public int deviceToStorageGroup(PartialPath deviceId);

  /**
   * release resource
   */
  public void clear();

  /**
   * get total number of virtual storage group
   *
   * @return total number of virtual storage group
   */
  public int getPartitionCount();

}