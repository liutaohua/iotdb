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

import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

public class Session {

  private final long sessionId;
  private final ZoneId zoneId;
  private final String userName;
  private final TSProtocolVersion version;
  private final IPlanExecutor executor;

  private AtomicLong statementIdGenerator = new AtomicLong();
  private Map<Long, QueryStatement> openedStatement = new ConcurrentHashMap<>();

  public Session(IPlanExecutor executor, long sessionId, String userName, ZoneId zoneId,
      TSProtocolVersion version) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.zoneId = zoneId;
    this.version = version;
    this.executor = executor;
  }

  public Statement openNoneQueryStatement() {
    return new Statement();
  }

  public QueryStatement openQueryStatement() {
    long statementId = statementIdGenerator.incrementAndGet();
    QueryStatement queryStatement = new QueryStatement(executor, statementId);
    openedStatement.put(statementId, queryStatement);
    return openedStatement.get(statementId);
  }


  public void close() {

  }

  public long getSessionId() {
    return sessionId;
  }

  public String getUserName() {
    return userName;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public Query searchQuery(long queryId) {
    for (QueryStatement value : openedStatement.values()) {
      Query query = value.getQuery(queryId);
      if (query != null) {
        return query;
      }
    }
    return null;
  }

  public QueryStatement getQueryStatement(long stmtId) {
    return openedStatement.get(stmtId);
  }
}
