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
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
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

    abstract void load(NodeCursor sourceNode, long localNodeId);

    void readOutgoingRelationships(
            VisitRelationship visit,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            NodeCursor sourceNode,
            long localGraphId) {

        int degree = loadRelationships.degreeOut(sourceNode);
        if (degree <= 0) {
            return;
        }

        visit.prepareNextNode(degree, localGraphId);
        visitOut(sourceNode, visit);

        long adjacencyIdx = visit.flush(allocator);
        if (adjacencyIdx != 0L) {
            offsets.set(localGraphId, adjacencyIdx);
        }
    }

    void readIncomingRelationships(
            VisitRelationship visit,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            NodeCursor sourceNode,
            long localGraphId) {

        int degree = loadRelationships.degreeIn(sourceNode);
        if (degree <= 0) {
            return;
        }

        visit.prepareNextNode(degree, localGraphId);
        visitIn(sourceNode, visit);

        long adjacencyIdx = visit.flush(allocator);
        if (adjacencyIdx != 0L) {
            offsets.set(localGraphId, adjacencyIdx);
        }
    }

    void readUndirected(
            VisitRelationship visitOut,
            VisitRelationship visitIn,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            NodeCursor sourceNode,
            long localGraphId) {

        int degree = loadRelationships.degreeBoth(sourceNode);
        if (degree <= 0) {
            return;
        }

        visitIn.prepareNextNode(degree, localGraphId);
        this.visitIn(sourceNode, visitIn);
        visitOut.prepareNextNode(visitIn);
        this.visitOut(sourceNode, visitOut);

        long adjacencyIdx = visitOut.flush(allocator);
        if (adjacencyIdx != 0L) {
            offsets.set(localGraphId, adjacencyIdx);
        }
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
    void load(final NodeCursor sourceNode, final long localNodeId) {
    }
}

final class ReadOutgoing extends RelationshipLoader {
    final VisitRelationship visitOutgoing;
    final HugeLongArray offsets;
    final ByteArray.LocalAllocator allocator;

    ReadOutgoing(
            final KernelTransaction transaction,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            final int[] relationType,
            final VisitRelationship visitOutgoing) {
        super(transaction, relationType);
        this.offsets = offsets;
        this.allocator = allocator;
        this.visitOutgoing = visitOutgoing;
    }

    @Override
    void load(final NodeCursor sourceNode, final long localNodeId) {
        readOutgoingRelationships(visitOutgoing, offsets, allocator, sourceNode, localNodeId);
    }
}

final class ReadIncoming extends RelationshipLoader {
    private final VisitRelationship visitIncoming;
    private final HugeLongArray offsets;
    private final ByteArray.LocalAllocator allocator;

    ReadIncoming(
            final KernelTransaction transaction,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            final int[] relationType,
            final VisitRelationship visitIncoming) {
        super(transaction, relationType);
        this.offsets = offsets;
        this.allocator = allocator;
        this.visitIncoming = visitIncoming;
    }

    @Override
    void load(final NodeCursor sourceNode, final long localNodeId) {
        readIncomingRelationships(visitIncoming, offsets, allocator, sourceNode, localNodeId);
    }
}

final class ReadBoth extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;
    private final HugeLongArray inOffsets;
    private final ByteArray.LocalAllocator inAllocator;
    private final HugeLongArray outOffsets;
    private final ByteArray.LocalAllocator outAllocator;

    ReadBoth(
            final ReadOutgoing readOut,
            final VisitRelationship visitIncoming,
            final HugeLongArray inOffsets,
            final ByteArray.LocalAllocator inAllocator) {
        super(readOut);
        this.visitOutgoing = readOut.visitOutgoing;
        this.visitIncoming = visitIncoming;
        this.outOffsets = readOut.offsets;
        this.outAllocator = readOut.allocator;
        this.inOffsets = inOffsets;
        this.inAllocator = inAllocator;
    }

    @Override
    void load(final NodeCursor sourceNode, final long localNodeId) {
        readOutgoingRelationships(visitOutgoing, outOffsets, outAllocator, sourceNode, localNodeId);
        readIncomingRelationships(visitIncoming, inOffsets, inAllocator, sourceNode, localNodeId);
    }
}

final class ReadUndirected extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;
    private final HugeLongArray offsets;
    private final ByteArray.LocalAllocator allocator;

    ReadUndirected(
            final KernelTransaction transaction,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            final int[] relationType,
            final VisitRelationship visitOutgoing,
            final VisitRelationship visitIncoming) {
        super(transaction, relationType);
        this.offsets = offsets;
        this.allocator = allocator;
        this.visitOutgoing = visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    void load(final NodeCursor sourceNode, final long localNodeId) {
        readUndirected(visitOutgoing, visitIncoming, offsets, allocator, sourceNode, localNodeId);
    }
}
