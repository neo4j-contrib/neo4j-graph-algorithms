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

import org.neo4j.graphalgo.core.HugeWeightMap;
import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;


abstract class VisitRelationship {

    private static final long[] EMPTY_LONGS = new long[0];

    private final HugeIdMap idMap;

    private long[] targets;
    private int length;
    private long prevNode;
    private boolean isSorted;

    long prevTarget;
    long sourceGraphId;

    VisitRelationship(final HugeIdMap idMap) {
        this.idMap = idMap;
        this.targets = EMPTY_LONGS;
    }

    abstract void visit(RelationshipSelectionCursor cursor);

    final void prepareNextNode(int degree, long sourceGraphId) {
        this.sourceGraphId = sourceGraphId;
        length = 0;
        prevTarget = -1L;
        prevNode = -1L;
        isSorted = true;
        if (targets.length < degree) {
            // give leeway in case of nodes with a reference to themselves
            // due to automatic skipping of identical targets, just adding one is enough to cover the
            // self-reference case, as it is handled as two relationships that aren't counted by BOTH
            // avoid repeated re-allocation for smaller degrees
            // avoid generous over-allocation for larger degrees
            int newSize = Math.max(32, 1 + degree);
            targets = new long[newSize];
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

    final long flush(HugeAdjacencyBuilder builder) {
        return builder.applyVariableDeltaEncoding(targets, length);
    }

    static void visitWeight(
            Read readOp,
            CursorFactory cursors,
            long sourceGraphId,
            long targetGraphId,
            HugeWeightMap weights,
            int weightProperty,
            long propertiesRef,
            long relationshipId) {
        try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
            readOp.relationshipProperties(relationshipId, propertiesRef, pc);
            double weight = ReadHelper.readProperty(pc, weightProperty, weights.defaultValue());
            if (weight != weights.defaultValue()) {
                weights.put(sourceGraphId, targetGraphId, weight);
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
            long propertiesRef,
            long relationshipId) {

        // TODO: make access to rel properties better
        try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
            readOp.relationshipProperties(relationshipId, propertiesRef, pc);
            double weight = ReadHelper.readProperty(pc, weightProperty, weights.defaultValue());
            if (weight != weights.defaultValue()) {
                weights.put(sourceGraphId, targetGraphId, weight);
                weights.put(targetGraphId, sourceGraphId, weight);
            }
        }
    }
}

final class VisitOutgoingNoWeight extends VisitRelationship {

    VisitOutgoingNoWeight(final HugeIdMap idMap) {
        super(idMap);
    }

    @Override
    public void visit(RelationshipSelectionCursor cursor) {
        addNode(cursor.targetNodeReference());
    }
}

final class VisitIncomingNoWeight extends VisitRelationship {

    VisitIncomingNoWeight(final HugeIdMap idMap) {
        super(idMap);
    }

    @Override
    public void visit(RelationshipSelectionCursor cursor) {
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
                    cursor.propertiesReference(),
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
                    cursor.propertiesReference(),
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
                    cursor.propertiesReference(),
                    cursor.relationshipReference());
        }
    }
}
