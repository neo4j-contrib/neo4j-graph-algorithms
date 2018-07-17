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

    abstract int load(
            final NodeCursor sourceNode,
            final long nodeId,
            final WeightBuilder weightBuilder,
            final AdjacencyBuilder outBuilder,
            final AdjacencyBuilder inBuilder);

    int readOutgoingRelationships(
            VisitRelationship visit,
            WeightBuilder weightBuilder,
            AdjacencyBuilder adjacencyBuilder,
            NodeCursor sourceNode,
            long nodeId) {

        int degree = loadRelationships.degreeOut(sourceNode);
        if (degree > 0) {
            visit.prepareNextNode(degree, nodeId);
            visitOut(sourceNode, visit, weightBuilder);
            return adjacencyBuilder.add(visit, nodeId);
        }
        return 0;
    }

    int readIncomingRelationships(
            VisitRelationship visit,
            WeightBuilder weightBuilder,
            AdjacencyBuilder adjacencyBuilder,
            NodeCursor sourceNode,
            long nodeId) {

        int degree = loadRelationships.degreeIn(sourceNode);
        if (degree > 0) {
            visit.prepareNextNode(degree, nodeId);
            visitIn(sourceNode, visit, weightBuilder);
            return adjacencyBuilder.add(visit, nodeId);
        }
        return 0;
    }

    int readUndirected(
            VisitRelationship visitOut,
            VisitRelationship visitIn,
            WeightBuilder weightBuilder,
            AdjacencyBuilder adjacencyBuilder,
            NodeCursor sourceNode,
            long nodeId) {

        int degree = loadRelationships.degreeBoth(sourceNode);
        if (degree > 0) {
            visitIn.prepareNextNode(degree, nodeId);
            this.visitIn(sourceNode, visitIn, weightBuilder);
            visitOut.prepareNextNode(visitIn);
            this.visitOut(sourceNode, visitOut, weightBuilder);
            return adjacencyBuilder.add(visitOut, nodeId);
        }
        return 0;
    }

    private void visitOut(
            NodeCursor cursor,
            VisitRelationship visit,
            WeightBuilder weightBuilder) {
        try (RelationshipSelectionCursor rc = loadRelationships.relationshipsOut(cursor)) {
            while (rc.next()) {
                visit.visit(rc, weightBuilder);
            }
        }
    }

    private void visitIn(
            NodeCursor cursor,
            VisitRelationship visit,
            WeightBuilder weightBuilder) {
        try (RelationshipSelectionCursor rc = loadRelationships.relationshipsIn(cursor)) {
            while (rc.next()) {
                visit.visit(rc, weightBuilder);
            }
        }
    }
}

final class ReadNothing extends RelationshipLoader {
    ReadNothing(final KernelTransaction transaction, final int[] relationType) {
        super(transaction, relationType);
    }

    @Override
    int load(
            NodeCursor sourceNode,
            long nodeId,
            WeightBuilder weightBuilder,
            AdjacencyBuilder outBuilder,
            AdjacencyBuilder inBuilder) {
        return 0;
    }
}

final class ReadOutgoing extends RelationshipLoader {
    final VisitRelationship visitOutgoing;

    ReadOutgoing(
            final KernelTransaction transaction,
            final int[] relationType,
            final VisitRelationship visitOutgoing) {
        super(transaction, relationType);
        this.visitOutgoing = visitOutgoing;
    }

    @Override
    int load(
            NodeCursor sourceNode,
            long nodeId,
            WeightBuilder weightBuilder,
            AdjacencyBuilder outBuilder,
            AdjacencyBuilder inBuilder) {
        return readOutgoingRelationships(
                visitOutgoing,
                weightBuilder,
                outBuilder,
                sourceNode,
                nodeId
        );
    }
}

final class ReadIncoming extends RelationshipLoader {
    private final VisitRelationship visitIncoming;

    ReadIncoming(
            final KernelTransaction transaction,
            final int[] relationType,
            final VisitRelationship visitIncoming) {
        super(transaction, relationType);
        this.visitIncoming = visitIncoming;
    }

    @Override
    int load(
            NodeCursor sourceNode,
            long nodeId,
            WeightBuilder weightBuilder,
            AdjacencyBuilder outBuilder,
            AdjacencyBuilder inBuilder) {
        return readIncomingRelationships(
                visitIncoming,
                weightBuilder,
                inBuilder,
                sourceNode,
                nodeId
        );
    }
}

final class ReadBoth extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;

    ReadBoth(
            final ReadOutgoing readOut,
            final VisitRelationship visitIncoming) {
        super(readOut);
        this.visitOutgoing = readOut.visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    int load(
            NodeCursor sourceNode,
            long nodeId,
            WeightBuilder weightBuilder,
            AdjacencyBuilder outBuilder,
            AdjacencyBuilder inBuilder) {
        return readOutgoingRelationships(
                visitOutgoing,
                weightBuilder,
                outBuilder,
                sourceNode,
                nodeId
        ) + readIncomingRelationships(
                visitIncoming,
                weightBuilder,
                inBuilder,
                sourceNode,
                nodeId
        );
    }
}

final class ReadUndirected extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;

    ReadUndirected(
            final KernelTransaction transaction,
            final int[] relationType,
            final VisitRelationship visitOutgoing,
            final VisitRelationship visitIncoming) {
        super(transaction, relationType);
        this.visitOutgoing = visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    int load(
            NodeCursor sourceNode,
            long nodeId,
            WeightBuilder weightBuilder,
            AdjacencyBuilder outBuilder,
            AdjacencyBuilder inBuilder) {
        return readUndirected(
                visitOutgoing,
                visitIncoming,
                weightBuilder,
                outBuilder,
                sourceNode,
                nodeId
        );
    }
}
