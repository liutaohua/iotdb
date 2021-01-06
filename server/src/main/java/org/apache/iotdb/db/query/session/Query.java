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

package org.apache.iotdb.db.query.session;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.UDTFDataSet;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;

public class Query {

  private final long queryId;

  private QueryContext context;
  private QueryDataSet queryDataSet;

  private IPlanExecutor executor;

  public Query(IPlanExecutor executor, long queryId) {
    this.executor = executor;
    this.queryId = queryId;
    this.context = new QueryContext(queryId);
  }

  public QueryDataSet query(PhysicalPlan plan) throws StorageEngineException,
      QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
      QueryProcessException, TException, SQLException {
    this.queryDataSet = executor.processQuery(plan, context);
    return queryDataSet;
  }

  public void close() throws IOException, StorageEngineException {
    // remove the corresponding Physical Plan
    if (queryDataSet instanceof UDTFDataSet) {
      ((UDTFDataSet) queryDataSet).finalizeUDFs(queryId);
    }
    QueryResourceManager.getInstance().endQuery(queryId);
  }

  public long getQueryId() {
    return queryId;
  }

  public QueryDataSet getQueryDataSet() {
    return queryDataSet;
  }
}
