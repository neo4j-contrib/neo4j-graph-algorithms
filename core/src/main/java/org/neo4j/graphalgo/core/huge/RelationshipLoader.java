package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

abstract class RelationshipLoader {
    private final ReadOperations readOp;
    private final int[] relationType;

    RelationshipLoader(
            final ReadOperations readOp,
            final int[] relationType) {
        this.readOp = readOp;
        this.relationType = relationType;
    }

    RelationshipLoader(final RelationshipLoader other) {
        this.readOp = other.readOp;
        this.relationType = other.relationType;
    }

    abstract void load(long sourceNodeId, long localNodeId) throws EntityNotFoundException;

    void readRelationship(
            VisitRelationship visit,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            Direction direction,
            long sourceNodeId,
            long localGraphId)
    throws EntityNotFoundException {

        int degree = degree(sourceNodeId, direction);
        if (degree <= 0) {
            return;
        }

        RelationshipIterator rs = rels(sourceNodeId, direction);
        visit.prepareNextNode(degree, localGraphId);
        while (rs.hasNext()) {
            rs.relationshipVisit(rs.next(), visit);
        }
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
            long sourceNodeId,
            long localGraphId)
    throws EntityNotFoundException {

        int degree = degree(sourceNodeId, Direction.BOTH);
        if (degree <= 0) {
            return;
        }

        RelationshipIterator rs = rels(sourceNodeId, Direction.INCOMING);
        visitIn.prepareNextNode(degree, localGraphId);
        while (rs.hasNext()) {
            rs.relationshipVisit(rs.next(), visitIn);
        }

        visitOut.prepareNextNode(visitIn);
        rs = rels(sourceNodeId, Direction.OUTGOING);
        while (rs.hasNext()) {
            rs.relationshipVisit(rs.next(), visitOut);
        }

        long adjacencyIdx = visitOut.flush(allocator);
        if (adjacencyIdx != 0L) {
            offsets.set(localGraphId, adjacencyIdx);
        }
    }

    private int degree(long nodeId, Direction direction) throws EntityNotFoundException {
        if (relationType == null) {
            return readOp.nodeGetDegree(nodeId, direction);
        }
        return readOp.nodeGetDegree(nodeId, direction, relationType[0]);
    }

    private RelationshipIterator rels(long nodeId, Direction direction) throws EntityNotFoundException {
        if (relationType == null) {
            return readOp.nodeGetRelationships(nodeId, direction);
        }
        return readOp.nodeGetRelationships(nodeId, direction, relationType);
    }
}

final class ReadNothing extends RelationshipLoader {
    ReadNothing(final ReadOperations readOp, final int[] relationType) {
        super(readOp, relationType);
    }

    @Override
    void load(final long sourceNodeId, final long localNodeId) {
    }
}

final class ReadOutgoing extends RelationshipLoader {
    final VisitRelationship visitOutgoing;
    final HugeLongArray offsets;
    final ByteArray.LocalAllocator allocator;

    ReadOutgoing(
            final ReadOperations readOp,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            final int[] relationType,
            final VisitRelationship visitOutgoing) {
        super(readOp, relationType);
        this.offsets = offsets;
        this.allocator = allocator;
        this.visitOutgoing = visitOutgoing;
    }

    @Override
    void load(final long sourceNodeId, final long localNodeId) throws EntityNotFoundException {
        readRelationship(visitOutgoing, offsets, allocator, Direction.OUTGOING, sourceNodeId, localNodeId);
    }
}

final class ReadIncoming extends RelationshipLoader {
    private final VisitRelationship visitIncoming;
    private final HugeLongArray offsets;
    private final ByteArray.LocalAllocator allocator;

    ReadIncoming(
            final ReadOperations readOp,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            final int[] relationType,
            final VisitRelationship visitIncoming) {
        super(readOp, relationType);
        this.offsets = offsets;
        this.allocator = allocator;
        this.visitIncoming = visitIncoming;
    }

    @Override
    void load(final long sourceNodeId, final long localNodeId) throws EntityNotFoundException {
        readRelationship(visitIncoming, offsets, allocator, Direction.INCOMING, sourceNodeId, localNodeId);
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
    void load(final long sourceNodeId, final long localNodeId) throws EntityNotFoundException {
        readRelationship(visitOutgoing, outOffsets, outAllocator, Direction.OUTGOING, sourceNodeId, localNodeId);
        readRelationship(visitIncoming, inOffsets, inAllocator, Direction.INCOMING, sourceNodeId, localNodeId);
    }
}

final class ReadUndirected extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;
    private final HugeLongArray offsets;
    private final ByteArray.LocalAllocator allocator;

    ReadUndirected(
            final ReadOperations readOp,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator,
            final int[] relationType,
            final VisitRelationship visitOutgoing,
            final VisitRelationship visitIncoming) {
        super(readOp, relationType);
        this.offsets = offsets;
        this.allocator = allocator;
        this.visitOutgoing = visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    void load(final long sourceNodeId, final long localNodeId) throws EntityNotFoundException {
        readUndirected(visitOutgoing, visitIncoming, offsets, allocator, sourceNodeId, localNodeId);
    }
}
