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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.query.control.QueryResourceManager;

public class QueryStatement extends Statement {

  private final long statementId;
  private IPlanExecutor executor;

  private QueryResourceManager resourceManager = QueryResourceManager.getInstance();
  private Map<Long, Query> openedQuery = new ConcurrentHashMap<>();

  public QueryStatement(IPlanExecutor executor, long statementId) {
    this.statementId = statementId;
    this.executor = executor;
  }

  public Query openQuery(int fetchSize, int deduplicatedPathNum) {
    long queryId = resourceManager.assignQueryId(true, fetchSize, deduplicatedPathNum);
    Query query = new Query(executor, queryId);
    openedQuery.put(queryId, query);
    return openedQuery.get(queryId);
  }

  @Override
  public void close() throws IOException, StorageEngineException {
    for (Query q : openedQuery.values()) {
      q.close();
    }
  }

  public Query getQuery(Long queryId) {
    return openedQuery.get(queryId);
  }
}
