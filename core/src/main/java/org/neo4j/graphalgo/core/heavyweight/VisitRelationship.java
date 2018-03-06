package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;

import java.util.Arrays;


abstract class VisitRelationship implements RelationshipVisitor<EntityNotFoundException> {

    private final IdMap idMap;
    private final boolean shouldSort;

    private int[] targets;
    private int length;
    private long prevNode;
    private boolean isSorted;

    int prevTarget;
    int sourceGraphId;

    VisitRelationship(final IdMap idMap, final boolean shouldSort) {
        this.idMap = idMap;
        this.shouldSort = shouldSort;
        if (!shouldSort) {
            isSorted = false;
        }
    }

    final void prepareNextNode(final int sourceGraphId, final int[] targets) {
        this.sourceGraphId = sourceGraphId;
        length = 0;
        prevTarget = -1;
        prevNode = -1L;
        isSorted = shouldSort;
        this.targets = targets;
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
        final int targetId = idMap.get(nodeId);
        if (targetId == -1) {
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

    final int flush() {
        if (shouldSort) {
            Arrays.sort(targets, 0, length);
            length = checkDistinct(targets, length);
        }
        return length;
    }

    static void visitWeight(
            ReadOperations readOp,
            int sourceGraphId,
            int targetGraphId,
            WeightMap weights,
            long relationshipId) {
        Object value;
        try {
            value = readOp.relationshipGetProperty(relationshipId, weights.propertyId());
        } catch (EntityNotFoundException ignored) {
            return;
        }
        if (value == null) {
            return;
        }
        double defaultValue = weights.defaultValue();
        double doubleValue = RawValues.extractValue(value, defaultValue);
        if (Double.compare(doubleValue, defaultValue) == 0) {
            return;
        }
        long relId = RawValues.combineIntInt(sourceGraphId, targetGraphId);
        weights.put(relId, doubleValue);
    }

    static void visitUndirectedWeight(
            ReadOperations readOp,
            int sourceGraphId,
            int targetGraphId,
            WeightMap weights,
            long relationshipId) {
        Object value;
        try {
            value = readOp.relationshipGetProperty(relationshipId, weights.propertyId());
        } catch (EntityNotFoundException ignored) {
            return;
        }
        if (value == null) {
            return;
        }
        double defaultValue = weights.defaultValue();
        double doubleValue = RawValues.extractValue(value, defaultValue);
        if (Double.compare(doubleValue, defaultValue) == 0) {
            return;
        }
        long relId = RawValues.combineIntInt(sourceGraphId, targetGraphId);
        weights.put(relId, doubleValue);
        relId = RawValues.combineIntInt(targetGraphId, sourceGraphId);
        weights.put(relId, doubleValue);
    }

    private static int checkDistinct(final int[] values, final int len) {
        int prev = -1;
        for (int i = 0; i < len; i++) {
            final int value = values[i];
            if (value == prev) {
                return distinct(values, i, len);
            }
            prev = value;
        }
        return len;
    }

    private static int distinct(final int[] values, final int start, final int len) {
        int prev = values[start - 1];
        int write = start;
        for (int i = start + 1; i < len; i++) {
            final int value = values[i];
            if (value > prev) {
                values[write++] = value;
            }
            prev = value;
        }
        return write;
    }
}

final class VisitOutgoingNoWeight extends VisitRelationship {

    VisitOutgoingNoWeight(final IdMap idMap, final boolean shouldSort) {
        super(idMap, shouldSort);
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        addNode(endNodeId);
    }
}

final class VisitIncomingNoWeight extends VisitRelationship {

    VisitIncomingNoWeight(final IdMap idMap, final boolean shouldSort) {
        super(idMap, shouldSort);
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        addNode(startNodeId);
    }
}

final class VisitOutgoingWithWeight extends VisitRelationship {

    private final ReadOperations readOp;
    private final WeightMap weights;

    VisitOutgoingWithWeight(
            final ReadOperations readOp,
            final IdMap idMap,
            final boolean shouldSort,
            final WeightMap weights) {
        super(idMap, shouldSort);
        this.readOp = readOp;
        this.weights = weights;
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        if (addNode(endNodeId)) {
            visitWeight(readOp, sourceGraphId, prevTarget, weights, relationshipId);
        }
    }
}

final class VisitIncomingWithWeight extends VisitRelationship {

    private final ReadOperations readOp;
    private final WeightMap weights;

    VisitIncomingWithWeight(
            final ReadOperations readOp,
            final IdMap idMap,
            final boolean shouldSort,
            final WeightMap weights) {
        super(idMap, shouldSort);
        this.readOp = readOp;
        this.weights = weights;
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        if (addNode(startNodeId)) {
            visitWeight(readOp, prevTarget, sourceGraphId, weights, relationshipId);
        }
    }
}

final class VisitUndirectedOutgoingWithWeight extends VisitRelationship {

    private final ReadOperations readOp;
    private final WeightMap weights;

    VisitUndirectedOutgoingWithWeight(
            final ReadOperations readOp,
            final IdMap idMap,
            final boolean shouldSort,
            final WeightMap weights) {
        super(idMap, shouldSort);
        this.readOp = readOp;
        this.weights = weights;
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        if (addNode(endNodeId)) {
            visitUndirectedWeight(readOp, sourceGraphId, prevTarget, weights, relationshipId);
        }
    }
}
