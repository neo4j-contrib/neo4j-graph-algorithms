package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

abstract class RelationshipLoader {
    private final ReadOperations readOp;
    private final AdjacencyMatrix matrix;
    private final int[] relationType;

    RelationshipLoader(
            final ReadOperations readOp,
            final AdjacencyMatrix matrix,
            final int[] relationType) {
        this.readOp = readOp;
        this.matrix = matrix;
        this.relationType = relationType;
    }

    RelationshipLoader(final RelationshipLoader other) {
        this.readOp = other.readOp;
        this.matrix = other.matrix;
        this.relationType = other.relationType;
    }

    abstract void load(long sourceNodeId, int localNodeId) throws EntityNotFoundException;

    void readOutgoing(
            VisitRelationship visit,
            long sourceNodeId,
            int localNodeId) throws EntityNotFoundException {
        final int outDegree = degree(sourceNodeId, Direction.OUTGOING);
        if (outDegree > 0) {
            final RelationshipIterator rels = rels(sourceNodeId, Direction.OUTGOING);
            final int[] targets = matrix.armOut(localNodeId, outDegree);
            visit.prepareNextNode(localNodeId, targets);
            while (rels.hasNext()) {
                final long relId = rels.next();
                rels.relationshipVisit(relId, visit);
            }
            matrix.setOutDegree(localNodeId, visit.flush());
        }
    }

    void readIncoming(
            VisitRelationship visit,
            long sourceNodeId,
            int localNodeId) throws EntityNotFoundException {
        final int inDegree = degree(sourceNodeId, Direction.INCOMING);
        if (inDegree > 0) {
            final RelationshipIterator rels = rels(sourceNodeId, Direction.INCOMING);
            final int[] targets = matrix.armIn(localNodeId, inDegree);
            visit.prepareNextNode(localNodeId, targets);
            while (rels.hasNext()) {
                final long relId = rels.next();
                rels.relationshipVisit(relId, visit);
            }
            matrix.setInDegree(localNodeId, visit.flush());
        }
    }

    void readUndirected(
            VisitRelationship visitOut,
            VisitRelationship visitIn,
            long sourceNodeId,
            int localNodeId) throws EntityNotFoundException {
        final int degree = degree(sourceNodeId, Direction.BOTH);
        if (degree > 0) {
            final int[] targets = matrix.armOut(localNodeId, degree);
            visitIn.prepareNextNode(localNodeId, targets);
            RelationshipIterator rels = rels(sourceNodeId, Direction.INCOMING);
            while (rels.hasNext()) {
                final long relId = rels.next();
                rels.relationshipVisit(relId, visitIn);
            }
            visitOut.prepareNextNode(visitIn);
            rels = rels(sourceNodeId, Direction.OUTGOING);
            while (rels.hasNext()) {
                final long relId = rels.next();
                rels.relationshipVisit(relId, visitOut);
            }
            matrix.setOutDegree(localNodeId, visitOut.flush());
        }
    }

    void readNodeWeight(
            long sourceNodeId,
            int sourceGraphId,
            WeightMap weights,
            int propertyId) {
        try {
            Object value = readOp.nodeGetProperty(sourceNodeId, propertyId);
            if (value != null) {
                weights.set(sourceGraphId, value);
            }
        } catch (EntityNotFoundException ignored) {
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
    ReadNothing(
            final ReadOperations readOp,
            final AdjacencyMatrix matrix,
            final int[] relationType) {
        super(readOp, matrix, relationType);
    }

    @Override
    void load(final long sourceNodeId, final int localNodeId) {
    }
}

final class ReadOutgoing extends RelationshipLoader {
    final VisitRelationship visitOutgoing;

    ReadOutgoing(
            final ReadOperations readOp,
            final AdjacencyMatrix matrix,
            final int[] relationType,
            final VisitRelationship visitOutgoing) {
        super(readOp, matrix, relationType);
        this.visitOutgoing = visitOutgoing;
    }

    @Override
    void load(final long sourceNodeId, final int localNodeId) throws EntityNotFoundException {
        readOutgoing(visitOutgoing, sourceNodeId, localNodeId);
    }
}

final class ReadIncoming extends RelationshipLoader {
    private final VisitRelationship visitIncoming;

    ReadIncoming(
            final ReadOperations readOp,
            final AdjacencyMatrix matrix,
            final int[] relationType,
            final VisitRelationship visitIncoming) {
        super(readOp, matrix, relationType);
        this.visitIncoming = visitIncoming;
    }

    @Override
    void load(final long sourceNodeId, final int localNodeId) throws EntityNotFoundException {
        readIncoming(visitIncoming, sourceNodeId, localNodeId);
    }
}

final class ReadBoth extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;

    ReadBoth(final ReadOutgoing readOut, final VisitRelationship visitIncoming) {
        super(readOut);
        this.visitOutgoing = readOut.visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    void load(final long sourceNodeId, final int localNodeId) throws EntityNotFoundException {
        readOutgoing(visitOutgoing, sourceNodeId, localNodeId);
        readIncoming(visitIncoming, sourceNodeId, localNodeId);
    }
}

final class ReadUndirected extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;

    ReadUndirected(
            final ReadOperations readOp,
            final AdjacencyMatrix matrix,
            final int[] relationType,
            final VisitRelationship visitOutgoing,
            final VisitRelationship visitIncoming) {
        super(readOp, matrix, relationType);
        this.visitOutgoing = visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    void load(final long sourceNodeId, final int localNodeId) throws EntityNotFoundException {
        readUndirected(visitOutgoing, visitIncoming, sourceNodeId, localNodeId);
    }
}

final class ReadWithNodeWeights extends RelationshipLoader {
    private final RelationshipLoader loader;
    private final WeightMap nodeWeights;

    ReadWithNodeWeights(
            final RelationshipLoader loader,
            final WeightMap nodeWeights) {
        super(loader);
        this.loader = loader;
        this.nodeWeights = nodeWeights;
    }

    @Override
    void load(final long sourceNodeId, final int localNodeId) throws EntityNotFoundException {
        loader.load(sourceNodeId, localNodeId);
        readNodeWeight(sourceNodeId, localNodeId, nodeWeights, nodeWeights.propertyId());
    }
}

final class ReadWithNodeWeightsAndProps extends RelationshipLoader {
    private final RelationshipLoader loader;
    private final WeightMap nodeWeights;
    private final WeightMap nodeProps;

    ReadWithNodeWeightsAndProps(
            final RelationshipLoader loader,
            final WeightMap nodeWeights,
            final WeightMap nodeProps) {
        super(loader);
        this.loader = loader;
        this.nodeWeights = nodeWeights;
        this.nodeProps = nodeProps;
    }

    @Override
    void load(final long sourceNodeId, final int localNodeId) throws EntityNotFoundException {
        loader.load(sourceNodeId, localNodeId);
        readNodeWeight(sourceNodeId, localNodeId, nodeWeights, nodeWeights.propertyId());
        readNodeWeight(sourceNodeId, localNodeId, nodeProps, nodeProps.propertyId());
    }
}
