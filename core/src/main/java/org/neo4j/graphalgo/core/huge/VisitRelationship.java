package org.neo4j.graphalgo.core.huge;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.core.HugeWeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.DeltaEncoding;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;

import java.util.Arrays;


abstract class VisitRelationship implements RelationshipVisitor<EntityNotFoundException> {

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
            ReadOperations readOp,
            long sourceGraphId,
            long targetGraphId,
            HugeWeightMap weights,
            int weightProperty,
            long relationshipId) {
        Object value;
        try {
            value = readOp.relationshipGetProperty(relationshipId, weightProperty);
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
        weights.put(sourceGraphId, targetGraphId, doubleValue);
    }

    static void visitUndirectedWeight(
            ReadOperations readOp,
            long sourceGraphId,
            long targetGraphId,
            HugeWeightMap weights,
            int weightProperty,
            long relationshipId) {
        Object value;
        try {
            value = readOp.relationshipGetProperty(relationshipId, weightProperty);
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
        weights.put(sourceGraphId, targetGraphId, doubleValue);
        weights.put(targetGraphId, sourceGraphId, doubleValue);
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
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        addNode(endNodeId);
    }
}

final class VisitIncomingNoWeight extends VisitRelationship {

    VisitIncomingNoWeight(final HugeIdMap idMap) {
        super(idMap);
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        addNode(startNodeId);
    }
}

final class VisitOutgoingWithWeight extends VisitRelationship {

    private final ReadOperations readOp;
    private final HugeWeightMap weights;
    private final int weightProperty;

    VisitOutgoingWithWeight(
            final ReadOperations readOp,
            final HugeIdMap idMap,
            final HugeWeightMap weights,
            final int weightProperty) {
        super(idMap);
        this.readOp = readOp;
        this.weights = weights;
        this.weightProperty = weightProperty;
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        if (addNode(endNodeId)) {
            visitWeight(readOp, sourceGraphId, prevTarget, weights, weightProperty, relationshipId);
        }
    }
}

final class VisitIncomingWithWeight extends VisitRelationship {

    private final ReadOperations readOp;
    private final HugeWeightMap weights;
    private final int weightProperty;

    VisitIncomingWithWeight(
            final ReadOperations readOp,
            final HugeIdMap idMap,
            final HugeWeightMap weights,
            final int weightProperty) {
        super(idMap);
        this.readOp = readOp;
        this.weights = weights;
        this.weightProperty = weightProperty;
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        if (addNode(startNodeId)) {
            visitWeight(readOp, prevTarget, sourceGraphId, weights, weightProperty, relationshipId);
        }
    }
}

final class VisitUndirectedOutgoingWithWeight extends VisitRelationship {

    private final ReadOperations readOp;
    private final HugeWeightMap weights;
    private final int weightProperty;

    VisitUndirectedOutgoingWithWeight(
            final ReadOperations readOp,
            final HugeIdMap idMap,
            final HugeWeightMap weights,
            final int weightProperty) {
        super(idMap);
        this.readOp = readOp;
        this.weights = weights;
        this.weightProperty = weightProperty;
    }

    @Override
    public void visit(final long relationshipId, final int typeId, final long startNodeId, final long endNodeId) {
        if (addNode(endNodeId)) {
            visitUndirectedWeight(readOp, sourceGraphId, prevTarget, weights, weightProperty, relationshipId);
        }
    }
}
