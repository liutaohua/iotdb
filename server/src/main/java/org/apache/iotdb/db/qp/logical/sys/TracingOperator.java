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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.TracingPlan;

public class TracingOperator extends RootOperator {

  private boolean isTracingOn;

  public TracingOperator(int tokenIntType, boolean isTracingOn) {
    super(tokenIntType);
    this.isTracingOn = isTracingOn;
    operatorType = OperatorType.TRACING;
  }

  public boolean isTracingOn() {
    return isTracingOn;
  }

  @Override
  public PhysicalPlan convert(int fetchSize) throws QueryProcessException {
    return new TracingPlan(isTracingOn());
  }
}
