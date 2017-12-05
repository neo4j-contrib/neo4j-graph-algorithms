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
package org.neo4j.graphalgo.core.heavyweight;

import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Supplier;


final class RelationshipImporter extends StatementTask<Void, EntityNotFoundException> {

    private final boolean sort;
    private IdMap idMap;
    private final PrimitiveIntIterable nodes;
    private WeightMapping relWeights;
    private WeightMapping nodeWeights;
    private WeightMapping nodeProps;
    private final ImportProgress progress;
    private final int[] relationId;
    private final boolean loadIncoming;
    private final boolean loadOutgoing;

    private AdjacencyMatrix matrix;
    private final int nodeOffset;
    private int currentNodeCount;

    private int sourceGraphId;

    RelationshipImporter(
            GraphDatabaseAPI api,
            GraphSetup setup,
            GraphDimensions dimensions,
            ImportProgress progress,
            int batchSize,
            int nodeOffset,
            IdMap idMap,
            PrimitiveIntIterable nodes,
            Supplier<WeightMapping> relWeights,
            Supplier<WeightMapping> nodeWeights,
            Supplier<WeightMapping> nodeProps,
            boolean sort) {
        super(api);
        int nodeSize = Math.min(batchSize, idMap.size() - nodeOffset);
        this.progress = progress;
        this.nodeOffset = nodeOffset;
        this.idMap = idMap;
        this.nodes = nodes;
        this.relWeights = relWeights.get();
        this.nodeWeights = nodeWeights.get();
        this.nodeProps = nodeProps.get();
        this.relationId = dimensions.relationId();
        loadIncoming = setup.loadIncoming;
        loadOutgoing = setup.loadOutgoing;
        this.matrix = new AdjacencyMatrix(nodeSize, loadIncoming, loadOutgoing, setup.sort);
        this.currentNodeCount = 0;
        this.sort = sort;
    }

    @Override
    public String threadName() {
        return String.format(
                "[Heavy] RelationshipImport (%d..%d)",
                nodeOffset,
                nodeOffset + matrix.capacity());
    }

    @Override
    public Void apply(final Statement statement) throws EntityNotFoundException {
        final ReadOperations readOp = statement.readOperations();
        final boolean loadIncoming = this.loadIncoming;
        final boolean loadOutgoing = this.loadOutgoing;

        RelationshipVisitor<EntityNotFoundException> visitOutgoing = null;
        RelationshipVisitor<EntityNotFoundException> visitIncoming = null;
        boolean shouldLoadWeights = relWeights instanceof WeightMap;
        boolean isBoth = loadIncoming && loadOutgoing;
        if (loadOutgoing) {
            if (shouldLoadWeights) {
                final WeightMap weights = (WeightMap) this.relWeights;
                visitOutgoing = ((relationshipId, typeId, startNodeId, endNodeId) ->
                        visitOutgoingWithWeight(
                                readOp,
                                isBoth,
                                sourceGraphId,
                                weights,
                                relationshipId,
                                endNodeId));
            } else {
                visitOutgoing = ((relationshipId, typeId, startNodeId, endNodeId) -> visitOutgoing(endNodeId));
            }
        }
        if (loadIncoming) {
            if (shouldLoadWeights) {
                final WeightMap weights = (WeightMap) this.relWeights;
                visitIncoming = ((relationshipId, typeId, startNodeId, endNodeId) ->
                        visitIncomingWithWeight(
                                readOp,
                                isBoth,
                                sourceGraphId,
                                weights,
                                relationshipId,
                                startNodeId));
            } else {
                visitIncoming = ((relationshipId, typeId, startNodeId, endNodeId) -> visitIncoming(startNodeId));
            }
        }

        PrimitiveIntIterator iterator = nodes.iterator();
        int nodeOffset = this.nodeOffset;
        int nodeCount = 0;
        while (iterator.hasNext()) {
            final int nodeId = iterator.next();
            final long sourceNodeId = idMap.toOriginalNodeId(nodeId);
            this.sourceGraphId = nodeId - nodeOffset;
            nodeCount++;
            readNode(
                    readOp,
                    sourceNodeId,
                    sourceGraphId,
                    matrix,
                    loadIncoming,
                    loadOutgoing,
                    visitOutgoing,
                    visitIncoming,
                    nodeWeights,
                    nodeProps,
                    relationId
            );
            progress.relProgress();
        }
        this.currentNodeCount = nodeCount;
        return null;
    }

    private void readNode(
            ReadOperations readOp,
            long sourceNodeId,
            int localNodeId,
            AdjacencyMatrix matrix,
            boolean loadIncoming,
            boolean loadOutgoing,
            RelationshipVisitor<EntityNotFoundException> visitOutgoing,
            RelationshipVisitor<EntityNotFoundException> visitIncoming,
            WeightMapping nodeWeights,
            WeightMapping nodeProps,
            int[] relationType) throws EntityNotFoundException {
        if (loadOutgoing) {
            readOutgoing(readOp, visitOutgoing, sourceNodeId, localNodeId, matrix, relationType);
        }
        if (loadIncoming) {
            readIncoming(readOp, visitIncoming, sourceNodeId, localNodeId, matrix, relationType);
        }
        if (nodeWeights instanceof WeightMap) {
            final WeightMap weights = (WeightMap) nodeWeights;
            readNodeWeight(readOp, sourceNodeId, localNodeId, weights, weights.propertyId());
        }
        if (nodeProps instanceof WeightMap) {
            final WeightMap weights = (WeightMap) nodeProps;
            readNodeWeight(readOp, sourceNodeId, localNodeId, weights, weights.propertyId());
        }
    }

    private void readOutgoing(
            ReadOperations readOp,
            RelationshipVisitor<EntityNotFoundException> visit,
            long sourceNodeId,
            int localNodeId,
            AdjacencyMatrix matrix,
            int[] relationType) throws EntityNotFoundException {
        final int outDegree;
        final RelationshipIterator rels;
        if (relationType == null) {
            outDegree = readOp.nodeGetDegree(sourceNodeId, Direction.OUTGOING);
            rels = readOp.nodeGetRelationships(sourceNodeId, Direction.OUTGOING);
        } else {
            outDegree = readOp.nodeGetDegree(sourceNodeId, Direction.OUTGOING, relationType[0]);
            rels = readOp.nodeGetRelationships(sourceNodeId, Direction.OUTGOING, relationType);
        }

        matrix.armOut(localNodeId, outDegree);
        while (rels.hasNext()) {
            final long relId = rels.next();
            rels.relationshipVisit(relId, visit);
        }
        if (sort) {
            matrix.sortOutgoing(localNodeId);
        }
    }

    private void readIncoming(
            ReadOperations readOp,
            RelationshipVisitor<EntityNotFoundException> visit,
            long sourceNodeId,
            int localNodeId,
            AdjacencyMatrix matrix,
            int[] relationType) throws EntityNotFoundException {
        final int outDegree;
        final RelationshipIterator rels;
        if (relationType == null) {
            outDegree = readOp.nodeGetDegree(sourceNodeId, Direction.INCOMING);
            rels = readOp.nodeGetRelationships(sourceNodeId, Direction.INCOMING);
        } else {
            outDegree = readOp.nodeGetDegree(sourceNodeId, Direction.INCOMING, relationType[0]);
            rels = readOp.nodeGetRelationships(sourceNodeId, Direction.INCOMING, relationType);
        }

        matrix.armIn(localNodeId, outDegree);
        while (rels.hasNext()) {
            final long relId = rels.next();
            rels.relationshipVisit(relId, visit);
        }
        if (sort) {
            matrix.sortIncoming(localNodeId);
        }
    }

    private void readNodeWeight(
            ReadOperations readOp,
            long sourceNodeId,
            int sourceGraphId,
            WeightMap weights,
            int propertyId)  {
        try {
            Object value = readOp.nodeGetProperty(sourceNodeId, propertyId);
            if (value != null) {
                weights.set(sourceGraphId, value);
            }
        } catch (EntityNotFoundException ignored) {
        }
    }

    private int visitOutgoing(long endNodeId) {
        final int targetGraphId = idMap.get(endNodeId);
        if (targetGraphId != -1) {
            matrix.addOutgoing(sourceGraphId, targetGraphId);
        }
        return targetGraphId;
    }

    private int visitOutgoingWithWeight(
            ReadOperations readOp,
            boolean isBoth,
            int sourceGraphId,
            WeightMap weights,
            long relationshipId,
            long endNodeId) throws EntityNotFoundException {
        final int targetGraphId = visitOutgoing(endNodeId);
        if (targetGraphId != -1) {
            visitWeight(readOp, isBoth, sourceGraphId, targetGraphId, weights, relationshipId);
        }
        return targetGraphId;
    }

    private int visitIncoming(long startNodeId) {
        final int startGraphId = idMap.get(startNodeId);
        if (startGraphId != -1) {
            matrix.addIncoming(startGraphId, sourceGraphId);
        }
        return startGraphId;
    }

    private int visitIncomingWithWeight(
            ReadOperations readOp,
            boolean isBoth,
            int sourceGraphId,
            WeightMap weights,
            long relationshipId,
            long startNodeId) throws EntityNotFoundException {
        final int targetGraphId = visitIncoming(startNodeId);
        if (targetGraphId != -1) {
            visitWeight(readOp, isBoth, sourceGraphId, targetGraphId, weights, relationshipId);
        }
        return targetGraphId;
    }

    private void visitWeight(
            ReadOperations readOp,
            boolean isBoth,
            int sourceGraphId,
            int targetGraphId,
            WeightMap weights,
            long relationshipId) throws EntityNotFoundException {
        Object value = readOp.relationshipGetProperty(relationshipId, weights.propertyId());
        if (value == null) {
            return;
        }
        double defaultValue = weights.defaultValue();
        double doubleValue = RawValues.extractValue(value, defaultValue);
        if (Double.compare(doubleValue, defaultValue) == 0) {
            return;
        }

        long relId = isBoth
                ? RawValues.combineSorted(sourceGraphId, targetGraphId)
                : RawValues.combineIntInt(sourceGraphId, targetGraphId);

        weights.put(relId, doubleValue);
    }

    Graph toGraph(final IdMap idMap) {
        return new HeavyGraph(
                idMap,
                matrix,
                relWeights,
                nodeWeights,
                nodeProps);
    }

    void writeInto(
            AdjacencyMatrix matrix,
            WeightMapping relWeights,
            WeightMapping nodeWeights,
            WeightMapping nodeProps) {
        matrix.addMatrix(this.matrix, nodeOffset, currentNodeCount);
        combineMaps(relWeights, this.relWeights, nodeOffset);
        combineMaps(nodeWeights, this.nodeWeights, nodeOffset);
        combineMaps(nodeProps, this.nodeProps, nodeOffset);
    }

    void release() {
        this.idMap = null;
        this.matrix = null;
        this.relWeights = null;
        this.nodeWeights = null;
        this.nodeProps = null;
    }

    private void combineMaps(WeightMapping global, WeightMapping local, int offset) {
        if (global instanceof WeightMap && local instanceof WeightMap) {
            WeightMap localWeights = (WeightMap) local;
            final LongDoubleMap localMap = localWeights.weights();
            WeightMap globalWeights = (WeightMap) global;
            final LongDoubleMap globalMap = globalWeights.weights();

            for (LongDoubleCursor cursor : localMap) {
                globalMap.put(cursor.key + offset, cursor.value);
            }
        }
    }
}
