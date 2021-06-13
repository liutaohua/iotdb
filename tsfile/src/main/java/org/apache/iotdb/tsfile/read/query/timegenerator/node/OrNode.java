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
package org.apache.iotdb.tsfile.read.query.timegenerator.node;

import java.io.IOException;

public class OrNode implements Node {

    private Node leftChild;
    private Node rightChild;

    private boolean hasCachedLeftTime;
    private long cachedLeftTime;
    private Comparable cachedLeftObject;
    private boolean hasCachedRightTime;
    private long cachedRightTime;
    private Comparable cachedRightObject;
    private boolean ascending = true;

    public OrNode(Node leftChild, Node rightChild) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.hasCachedLeftTime = false;
        this.hasCachedRightTime = false;
    }

    public OrNode(Node leftChild, Node rightChild, boolean ascending) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.hasCachedLeftTime = false;
        this.hasCachedRightTime = false;
        this.ascending = ascending;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedLeftTime || hasCachedRightTime) {
            return true;
        }
        return leftChild.hasNext() || rightChild.hasNext();
    }

    private boolean hasLeftValue() throws IOException {
        return hasCachedLeftTime || leftChild.hasNext();
    }

    private long getLeftValue() throws IOException {
        if (hasCachedLeftTime) {
            hasCachedLeftTime = false;
            return cachedLeftTime;
        }
        return leftChild.next();
    }

    private Comparable getLeftObjectValue() throws IOException {
        if (hasCachedLeftTime) {
            hasCachedLeftTime = false;
            return cachedLeftObject;
        }
        return leftChild.nextObject();
    }

    private boolean hasRightValue() throws IOException {
        return hasCachedRightTime || rightChild.hasNext();
    }

    private long getRightValue() throws IOException {
        if (hasCachedRightTime) {
            hasCachedRightTime = false;
            return cachedRightTime;
        }
        return rightChild.next();
    }

    private Comparable getRightObjectValue() throws IOException {
        if (hasCachedRightTime) {
            hasCachedRightTime = false;
            return cachedRightObject;
        }
        return rightChild.nextObject();
    }

    @Override
    public long next() throws IOException {
        if (hasLeftValue() && !hasRightValue()) {
            return getLeftValue();
        } else if (!hasLeftValue() && hasRightValue()) {
            return getRightValue();
        } else if (hasLeftValue() && hasRightValue()) {
            long leftValue = getLeftValue();
            long rightValue = getRightValue();
            if (ascending) {
                return popAndFillNextCache(
                        leftValue < rightValue, leftValue > rightValue, leftValue, rightValue);
            }
            return popAndFillNextCache(
                    leftValue > rightValue, leftValue < rightValue, leftValue, rightValue);
        }
        throw new IOException("no more data");
    }

    @Override
    public Comparable nextObject() throws IOException {
        if (hasLeftValue() && !hasRightValue()) {
            return getLeftObjectValue();
        } else if (!hasLeftValue() && hasRightValue()) {
            return getRightObjectValue();
        } else if (hasLeftValue() && hasRightValue()) {
            Comparable leftValue = getLeftObjectValue();
            Comparable rightValue = getRightObjectValue();

            return popAndFillObjectNextCache(leftValue.compareTo(rightValue) > 0,
                    rightValue.compareTo(leftValue) > 0, leftValue, rightValue);
        }
        throw new IOException("no more data");
    }

    private Comparable popAndFillObjectNextCache(boolean popLeft, boolean popRight, Comparable left, Comparable right) {
        if (popLeft) {
            hasCachedRightTime = true;
            cachedRightObject = right;
            return left;
        } else if (popRight) {
            hasCachedLeftTime = true;
            cachedLeftObject = left;
            return right;
        } else {
            return left;
        }
    }

    private long popAndFillNextCache(boolean popLeft, boolean popRight, long left, long right) {
        if (popLeft) {
            hasCachedRightTime = true;
            cachedRightTime = right;
            return left;
        } else if (popRight) {
            hasCachedLeftTime = true;
            cachedLeftTime = left;
            return right;
        } else {
            return left;
        }
    }

    @Override
    public NodeType getType() {
        return NodeType.OR;
    }
}
