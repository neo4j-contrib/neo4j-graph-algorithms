/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.huge;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.core.HugeWeightMap;
import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.DeltaEncoding;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;

import java.util.Arrays;


abstract class VisitRelationship {

    private final HugeIdMap idMap;

    private long[] targets;
    private int length;
    private long prevNode;
    private boolean isSorted;

    long prevTarget;
    long sourceGraphId;

    VisitRelationship(final HugeIdMap idMap) {
        this.idMap = idMap;
        this.targets = new long[0];
    }

    abstract void visit(RelationshipSelectionCursor cursor);

    final void prepareNextNode(int degree, long sourceGraphId) {
        this.sourceGraphId = sourceGraphId;
        length = 0;
        prevTarget = -1L;
        prevNode = -1L;
        isSorted = true;
        if (targets.length < degree) {
            targets = new long[ArrayUtil.oversize(degree, Long.BYTES)];
        }
    }

    final void prepareNextNode(VisitRelationship other) {
        this.sourceGraphId = other.sourceGraphId;
        length = other.length;
        prevTarget = other.prevTarget;
        prevNode = other.prevNode;
        isSorted = other.isSorted;
        targets = other.targets;
    }

    final boolean addNode(final long nodeId) {
        if (nodeId == prevNode) {
            return false;
        }
        final long targetId = idMap.toHugeMappedNodeId(nodeId);
        if (targetId == -1L) {
            return false;
        }
        if (isSorted && targetId < prevTarget) {
            isSorted = false;
        }
        targets[length++] = targetId;
        prevNode = nodeId;
        prevTarget = targetId;
        return true;
    }

    final long flush(ByteArray.LocalAllocator allocator) {
        long requiredSize = applyDelta();
        int degree = length;
        if (degree == 0) {
            return 0L;
        }

        long adjacencyIdx = allocator.allocate(requiredSize);
        ByteArray.BulkAdder bulkAdder = allocator.adder;
        bulkAdder.addUnsignedInt(degree);
        long[] targets = this.targets;
        for (int i = 0; i < degree; i++) {
            bulkAdder.addVLong(targets[i]);
        }

        return adjacencyIdx;
    }

    static void visitWeight(
            Read readOp,
            CursorFactory cursors,
            long sourceGraphId,
            long targetGraphId,
            HugeWeightMap weights,
            int weightProperty,
            long relationshipId) {

        // TODO: make access to rel properties better
        try (RelationshipScanCursor scanCursor = cursors.allocateRelationshipScanCursor();
             PropertyCursor pc = cursors.allocatePropertyCursor()) {
            readOp.singleRelationship(relationshipId, scanCursor);
            while (scanCursor.next()) {
                scanCursor.properties(pc);
                double weight = ReadHelper.readProperty(pc, weightProperty, weights.defaultValue());
                if (weight != weights.defaultValue()) {
                    weights.put(sourceGraphId, targetGraphId, weight);
                }
            }
        }
    }

    static void visitUndirectedWeight(
            Read readOp,
            CursorFactory cursors,
            long sourceGraphId,
            long targetGraphId,
            HugeWeightMap weights,
            int weightProperty,
            long relationshipId) {

        // TODO: make access to rel properties better
        try (RelationshipScanCursor scanCursor = cursors.allocateRelationshipScanCursor();
             PropertyCursor pc = cursors.allocatePropertyCursor()) {
            readOp.singleRelationship(relationshipId, scanCursor);
            while (scanCursor.next()) {
                scanCursor.properties(pc);
                double weight = ReadHelper.readProperty(pc, weightProperty, weights.defaultValue());
                if (weight != weights.defaultValue()) {
                    weights.put(sourceGraphId, targetGraphId, weight);
                    weights.put(targetGraphId, sourceGraphId, weight);
                }
            }
        }
    }

    private long applyDelta() {
        int length = this.length;
        if (length == 0) {
            return 0L;
        }

        long[] targets = this.targets;
        if (!isSorted) {
            Arrays.sort(targets, 0, length);
        }

        long delta = targets[0];
        int writePos = 1;
        long requiredBytes = 4L + DeltaEncoding.vSize(delta);  // length as full-int

        for (int i = 1; i < length; ++i) {
            long nextDelta = targets[i];
            long value = targets[writePos] = nextDelta - delta;
            if (value > 0L) {
                ++writePos;
                requiredBytes += DeltaEncoding.vSize(value);
                delta = nextDelta;
            }
        }

        this.length = writePos;
        return requiredBytes;
    }
}

final class VisitOutgoingNoWeight extends VisitRelationship {

    VisitOutgoingNoWeight(final HugeIdMap idMap) {
        super(idMap);
    }

    @Override
    public void visit(final RelationshipSelectionCursor cursor) {
        addNode(cursor.targetNodeReference());
    }
}

final class VisitIncomingNoWeight extends VisitRelationship {

    VisitIncomingNoWeight(final HugeIdMap idMap) {
        super(idMap);
    }

    @Override
    public void visit(final RelationshipSelectionCursor cursor) {
        addNode(cursor.sourceNodeReference());
    }
}

final class VisitOutgoingWithWeight extends VisitRelationship {

    private final Read readOp;
    private final CursorFactory cursors;
    private final HugeWeightMap weights;
    private final int weightProperty;

    VisitOutgoingWithWeight(
            final Read readOp,
            final CursorFactory cursors,
            final HugeIdMap idMap,
            final HugeWeightMap weights,
            final int weightProperty) {
        super(idMap);
        this.readOp = readOp;
        this.cursors = cursors;
        this.weights = weights;
        this.weightProperty = weightProperty;
    }

    @Override
    public void visit(final RelationshipSelectionCursor cursor) {
        if (addNode(cursor.targetNodeReference())) {
            visitWeight(
                    readOp,
                    cursors,
                    sourceGraphId,
                    prevTarget,
                    weights,
                    weightProperty,
                    cursor.relationshipReference());
        }
    }
}

final class VisitIncomingWithWeight extends VisitRelationship {

    private final Read readOp;
    private final CursorFactory cursors;
    private final HugeWeightMap weights;
    private final int weightProperty;

    VisitIncomingWithWeight(
            final Read readOp,
            final CursorFactory cursors,
            final HugeIdMap idMap,
            final HugeWeightMap weights,
            final int weightProperty) {
        super(idMap);
        this.readOp = readOp;
        this.cursors = cursors;
        this.weights = weights;
        this.weightProperty = weightProperty;
    }

    @Override
    public void visit(final RelationshipSelectionCursor cursor) {
        if (addNode(cursor.sourceNodeReference())) {
            visitWeight(
                    readOp,
                    cursors,
                    prevTarget,
                    sourceGraphId,
                    weights,
                    weightProperty,
                    cursor.relationshipReference());
        }
    }
}

final class VisitUndirectedOutgoingWithWeight extends VisitRelationship {

    private final Read readOp;
    private final CursorFactory cursors;
    private final HugeWeightMap weights;
    private final int weightProperty;

    VisitUndirectedOutgoingWithWeight(
            final Read readOp,
            final CursorFactory cursors,
            final HugeIdMap idMap,
            final HugeWeightMap weights,
            final int weightProperty) {
        super(idMap);
        this.readOp = readOp;
        this.cursors = cursors;
        this.weights = weights;
        this.weightProperty = weightProperty;
    }

    @Override
    public void visit(final RelationshipSelectionCursor cursor) {
        if (addNode(cursor.targetNodeReference())) {
            visitUndirectedWeight(
                    readOp,
                    cursors,
                    sourceGraphId,
                    prevTarget,
                    weights,
                    weightProperty,
                    cursor.relationshipReference());
        }
    }
}
