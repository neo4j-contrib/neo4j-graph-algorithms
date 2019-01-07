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

import org.neo4j.graphalgo.core.loading.LoadRelationships;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.kernel.api.KernelTransaction;

abstract class RelationshipLoader {
    private final KernelTransaction transaction;
    private final LoadRelationships loadRelationships;
    private final int[] relationType;

    RelationshipLoader(
            final KernelTransaction transaction,
            final int[] relationType) {
        this.transaction = transaction;
        this.relationType = relationType;
        loadRelationships = LoadRelationships.of(transaction.cursors(), relationType);
    }

    RelationshipLoader(final RelationshipLoader other) {
        this.transaction = other.transaction;
        this.relationType = other.relationType;
        this.loadRelationships = other.loadRelationships;
    }

    abstract int load(NodeCursor sourceNode, long localNodeId);

    int readOutgoingRelationships(
            VisitRelationship visit,
            HugeLongArray offsets,
            HugeAdjacencyBuilder builder,
            NodeCursor sourceNode,
            long localGraphId) {

        int degree = loadRelationships.degreeOut(sourceNode);
        if (degree > 0) {

            visit.prepareNextNode(degree, localGraphId);
            visitOut(sourceNode, visit);

            long adjacencyIdx = visit.flush(builder);
            if (adjacencyIdx != 0L) {
                offsets.set(localGraphId, adjacencyIdx);
            }
        }
        return degree;
    }

    int readIncomingRelationships(
            VisitRelationship visit,
            HugeLongArray offsets,
            HugeAdjacencyBuilder builder,
            NodeCursor sourceNode,
            long localGraphId) {

        int degree = loadRelationships.degreeIn(sourceNode);
        if (degree > 0) {

            visit.prepareNextNode(degree, localGraphId);
            visitIn(sourceNode, visit);

            long adjacencyIdx = visit.flush(builder);
            if (adjacencyIdx != 0L) {
                offsets.set(localGraphId, adjacencyIdx);
            }
        }
        return degree;
    }

    int readUndirected(
            VisitRelationship visitOut,
            VisitRelationship visitIn,
            HugeLongArray offsets,
            HugeAdjacencyBuilder builder,
            NodeCursor sourceNode,
            long localGraphId) {

        int degree = loadRelationships.degreeUndirected(sourceNode);
        if (degree > 0) {

            visitIn.prepareNextNode(degree, localGraphId);
            this.visitIn(sourceNode, visitIn);
            visitOut.prepareNextNode(visitIn);
            this.visitOut(sourceNode, visitOut);

            long adjacencyIdx = visitOut.flush(builder);
            if (adjacencyIdx != 0L) {
                offsets.set(localGraphId, adjacencyIdx);
            }
        }
        return degree;
    }

    private void visitOut(NodeCursor cursor, VisitRelationship visit) {
        try (RelationshipSelectionCursor rc = loadRelationships.relationshipsOut(cursor)) {
            while (rc.next()) {
                visit.visit(rc);
            }
        }
    }

    private void visitIn(NodeCursor cursor, VisitRelationship visit) {
        try (RelationshipSelectionCursor rc = loadRelationships.relationshipsIn(cursor)) {
            while (rc.next()) {
                visit.visit(rc);
            }
        }
    }
}

final class ReadNothing extends RelationshipLoader {
    ReadNothing(final KernelTransaction transaction, final int[] relationType) {
        super(transaction, relationType);
    }

    @Override
    int load(final NodeCursor sourceNode, final long localNodeId) {
        return 0;
    }
}

final class ReadOutgoing extends RelationshipLoader {
    final VisitRelationship visitOutgoing;
    final HugeLongArray offsets;
    final HugeAdjacencyBuilder builder;

    ReadOutgoing(
            final KernelTransaction transaction,
            HugeLongArray offsets,
            HugeAdjacencyBuilder builder,
            final int[] relationType,
            final VisitRelationship visitOutgoing) {
        super(transaction, relationType);
        this.offsets = offsets;
        this.builder = builder;
        this.visitOutgoing = visitOutgoing;
    }

    @Override
    int load(final NodeCursor sourceNode, final long localNodeId) {
        return readOutgoingRelationships(visitOutgoing, offsets, builder, sourceNode, localNodeId);
    }
}

final class ReadIncoming extends RelationshipLoader {
    private final VisitRelationship visitIncoming;
    private final HugeLongArray offsets;
    private final HugeAdjacencyBuilder builder;

    ReadIncoming(
            final KernelTransaction transaction,
            HugeLongArray offsets,
            HugeAdjacencyBuilder builder,
            final int[] relationType,
            final VisitRelationship visitIncoming) {
        super(transaction, relationType);
        this.offsets = offsets;
        this.builder = builder;
        this.visitIncoming = visitIncoming;
    }

    @Override
    int load(final NodeCursor sourceNode, final long localNodeId) {
        return readIncomingRelationships(visitIncoming, offsets, builder, sourceNode, localNodeId);
    }
}

final class ReadBoth extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;
    private final HugeLongArray inOffsets;
    private final HugeAdjacencyBuilder inBuilder;
    private final HugeLongArray outOffsets;
    private final HugeAdjacencyBuilder outBuilder;

    ReadBoth(
            final ReadOutgoing readOut,
            final VisitRelationship visitIncoming,
            final HugeLongArray inOffsets,
            final HugeAdjacencyBuilder inBuilder) {
        super(readOut);
        this.visitOutgoing = readOut.visitOutgoing;
        this.visitIncoming = visitIncoming;
        this.outOffsets = readOut.offsets;
        this.outBuilder = readOut.builder;
        this.inOffsets = inOffsets;
        this.inBuilder = inBuilder;
    }

    @Override
    int load(final NodeCursor sourceNode, final long localNodeId) {
        return readOutgoingRelationships(visitOutgoing, outOffsets, outBuilder, sourceNode, localNodeId) +
                readIncomingRelationships(visitIncoming, inOffsets, inBuilder, sourceNode, localNodeId);
    }
}

final class ReadUndirected extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;
    private final HugeLongArray offsets;
    private final HugeAdjacencyBuilder builder;

    ReadUndirected(
            final KernelTransaction transaction,
            HugeLongArray offsets,
            HugeAdjacencyBuilder builder,
            final int[] relationType,
            final VisitRelationship visitOutgoing,
            final VisitRelationship visitIncoming) {
        super(transaction, relationType);
        this.offsets = offsets;
        this.builder = builder;
        this.visitOutgoing = visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    int load(final NodeCursor sourceNode, final long localNodeId) {
        return readUndirected(visitOutgoing, visitIncoming, offsets, builder, sourceNode, localNodeId);
    }
}
