package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public final class LightGraphFactory extends GraphFactory {

    private final ExecutorService threadPool;
    private int nodeCount;
    private int labelId;
    private int[] relationId;
    private int weightId;

    public LightGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
        setLog(setup.log);
        this.threadPool = setup.executor;
        withReadOps(readOp -> {
            labelId = setup.loadAnyLabel()
                    ? ReadOperations.ANY_LABEL
                    : readOp.labelGetForName(setup.startLabel);
            if (!setup.loadAnyRelationshipType()) {
                int relId = readOp.relationshipTypeGetForName(setup.relationshipType);
                if (relId != StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                    relationId = new int[]{relId};
                }
            }
            weightId = setup.loadDefaultRelationshipWeight()
                    ? StatementConstants.NO_SUCH_PROPERTY_KEY
                    : readOp.propertyKeyGetForName(setup.relationWeightPropertyName);
            nodeCount = Math.toIntExact(readOp.countsForNode(labelId));
        });
    }

    @Override
    public Graph build() {
        return build(ParallelUtil.DEFAULT_BATCH_SIZE);
    }

    /* test-private */ Graph build(int batchSize) {
        boolean loadIncoming = setup.loadIncoming;
        boolean loadOutgoing = setup.loadOutgoing;

        final IdMap mapping;
        final WeightMapping weights;
        long[] inOffsets = null;
        long[] outOffsets = null;
        IntArray inAdjacency = null;
        IntArray outAdjacency = null;

        mapping = new IdMap(nodeCount);
        // we allocate one more offset in order to avoid having to
        // check for the last element during degree access
        if (loadIncoming) {
            inOffsets = new long[nodeCount + 1];
            inAdjacency = IntArray.newArray(nodeCount);
        }
        if (loadOutgoing) {
            outOffsets = new long[nodeCount + 1];
            outAdjacency = IntArray.newArray(nodeCount);
        }
        weights = weightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.relationDefaultWeight)
                : new WeightMap(nodeCount, setup.relationDefaultWeight);

        int factor = ((nodeCount / 10) + 1);
        int fp2 = 1 << (32 - Integer.numberOfLeadingZeros(factor - 1) - 1);
        int mod = fp2 - 1;

        final long[] finalInOffsets = inOffsets;
        final long[] finalOutOffsets = outOffsets;
        final IntArray finalInAdjacency = inAdjacency;
        final IntArray finalOutAdjacency = outAdjacency;

        withReadOps(readOp -> {
            final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                    ? readOp.nodesGetAll()
                    : readOp.nodesGetForLabel(labelId);
            int nodes = 0;
            int inOffset = 0;
            int outOffset = 0;
            try {
                while (nodeIds.hasNext()) {
                    long neoId = nodeIds.next();
                    int graphId = mapping.add(neoId);
                    if (loadIncoming) {
                        int degree = relationId == null
                                ? readOp.nodeGetDegree(neoId, org.neo4j.graphdb.Direction.INCOMING)
                                : readOp.nodeGetDegree(neoId, org.neo4j.graphdb.Direction.INCOMING, relationId[0]);
                        finalInOffsets[graphId] = inOffset;
                        inOffset += degree;
                    }
                    if (loadOutgoing) {
                        int degree = relationId == null
                                ? readOp.nodeGetDegree(neoId, org.neo4j.graphdb.Direction.OUTGOING)
                                : readOp.nodeGetDegree(neoId, org.neo4j.graphdb.Direction.OUTGOING, relationId[0]);
                        finalOutOffsets[graphId] = outOffset;
                        outOffset += degree;
                    }
                    if ((++nodes & mod) == 0) {
                        progressLogger.logProgress(nodes, nodeCount);
                    }
                }
                if (loadIncoming) {
                    finalInOffsets[nodeCount] = inOffset;
                }
                if (loadOutgoing) {
                    finalOutOffsets[nodeCount] = outOffset;
                }
            } catch (EntityNotFoundException e) {
                throw new RuntimeException(e);
            }
            mapping.buildMappedIds();
        });

        ParallelUtil.readParallel(
                batchSize,
                mapping,
                (offset, nodeIds) -> new BatchImportTask(
                        offset,
                        nodeCount,
                        mod,
                        nodeIds,
                        mapping,
                        finalInOffsets,
                        finalOutOffsets,
                        finalInAdjacency,
                        finalOutAdjacency,
                        weights,
                        relationId,
                        weightId
                ),
                threadPool);

        progressLogger.logDone();

        return new LightGraph(
                mapping,
                weights,
                inAdjacency,
                outAdjacency,
                inOffsets,
                outOffsets
        );
    }

    private final class BatchImportTask implements Runnable, Consumer<ReadOperations> {
        private final int nodeOffset;
        private final int maxNodeId;
        private final int progressMod;
        private final PrimitiveIntIterable nodes;
        private final IdMap idMap;
        private final long[] inOffsets;
        private final long[] outOffsets;
        private final IntArray inAdjacency;
        private final IntArray outAdjacency;
        private final IntArray.BulkAdder inAdder;
        private final IntArray.BulkAdder outAdder;
        private final WeightMapping weights;
        private final int[] relationId;
        private final int weightId;

        BatchImportTask(
                int nodeOffset,
                int nodeCount,
                int progressMod,
                PrimitiveIntIterable nodes,
                IdMap idMap,
                long[] inOffsets,
                long[] outOffsets,
                IntArray inAdjacency,
                IntArray outAdjacency,
                WeightMapping weights,
                int[] relationId,
                int weightId) {
            this.nodeOffset = nodeOffset;
            this.maxNodeId = nodeCount - 1;
            this.progressMod = progressMod;
            this.nodes = nodes;
            this.idMap = idMap;
            this.inOffsets = inOffsets;
            this.outOffsets = outOffsets;
            this.inAdjacency = inAdjacency;
            this.outAdjacency = outAdjacency;
            this.inAdder = inAdjacency != null ? inAdjacency.newBulkAdder() : null;
            this.outAdder = outAdjacency != null ? outAdjacency.newBulkAdder() : null;
            this.weights = weights;
            this.relationId = relationId;
            this.weightId = weightId;
        }

        @Override
        public void run() {
            withReadOps(this);
        }

        @Override
        public void accept(ReadOperations readOp) {
            PrimitiveIntIterator iterator = nodes.iterator();
            boolean loadIncoming = inAdjacency != null;
            boolean loadOutgoing = outAdjacency != null;
            int nodes = 0;
            while (iterator.hasNext()) {
                int nodeId = iterator.next();
                try (Cursor<NodeItem> cursor = readOp.nodeCursor(idMap.toOriginalNodeId(nodeId))) {
                    if (cursor.next()) {
                        readNodeBatch(cursor.get(), loadIncoming, loadOutgoing);
                        if ((++nodes & progressMod) == 0) {
                            progressLogger.logProgress(nodes + nodeOffset, maxNodeId);
                        }
                    }
                }
            }
        }

        private void readNodeBatch(
                NodeItem node,
                boolean loadIncoming,
                boolean loadOutgoing) {
            long sourceNodeId = node.id();
            int sourceGraphId = idMap.get(sourceNodeId);

            if (loadOutgoing) {
                readRelationshipsBatch(
                        sourceGraphId,
                        node,
                        Direction.OUTGOING,
                        RawValues.OUTGOING,
                        outOffsets,
                        outAdder
                );
            }
            if (loadIncoming) {
                readRelationshipsBatch(
                        sourceGraphId,
                        node,
                        Direction.INCOMING,
                        RawValues.INCOMING,
                        inOffsets,
                        inAdder
                );
            }
        }

        private void readRelationshipsBatch(
                int sourceGraphId,
                NodeItem node,
                Direction direction,
                IdCombiner idCombiner,
                long[] offsets,
                IntArray.BulkAdder bulkAdder) {

            long adjacencyIndex = offsets[sourceGraphId];
            int degree = (int) (offsets[sourceGraphId + 1] - adjacencyIndex);

            if (degree > 0) {
                bulkAdder.init(adjacencyIndex, degree);
                try (Cursor<RelationshipItem> rels = relationId == null
                        ? node.relationships(direction)
                        : node.relationships(direction, relationId)) {
                    while (rels.next()) {
                        RelationshipItem rel = rels.get();

                        long targetNodeId = rel.otherNode(node.id());
                        int targetGraphId = idMap.get(targetNodeId);
                        if (targetGraphId == -1) {
                            continue;
                        }

                        try (Cursor<PropertyItem> weights = rel.property(
                                weightId)) {
                            if (weights.next()) {
                                long relId = idCombiner.apply(
                                        sourceGraphId,
                                        targetGraphId);
                                this.weights.set(relId, weights.get().value());
                            }
                        }
                        bulkAdder.add(targetGraphId);
                    }
                }
            }
        }
    }
}
