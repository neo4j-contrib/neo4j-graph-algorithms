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
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Supplier;


final class RelationshipImporter extends StatementTask<Void, EntityNotFoundException> {

    private final PrimitiveIntIterable nodes;
    private final GraphSetup setup;
    private final ImportProgress progress;
    private final int[] relationId;

    private final int nodeSize;
    private final int nodeOffset;

    private IdMap idMap;
    private AdjacencyMatrix matrix;
    private WeightMapping relWeights;
    private WeightMapping nodeWeights;
    private WeightMapping nodeProps;

    RelationshipImporter(
            GraphDatabaseAPI api,
            GraphSetup setup,
            GraphDimensions dimensions,
            ImportProgress progress,
            int batchSize,
            int nodeOffset,
            IdMap idMap,
            AdjacencyMatrix matrix,
            PrimitiveIntIterable nodes,
            Supplier<WeightMapping> relWeights,
            Supplier<WeightMapping> nodeWeights,
            Supplier<WeightMapping> nodeProps) {
        super(api);
        this.matrix = matrix;
        this.nodeSize = Math.min(batchSize, idMap.size() - nodeOffset);
        this.nodeOffset = nodeOffset;
        this.progress = progress;
        this.idMap = idMap;
        this.nodes = nodes;
        this.setup = setup;
        this.relWeights = relWeights.get();
        this.nodeWeights = nodeWeights.get();
        this.nodeProps = nodeProps.get();
        this.relationId = dimensions.relationId();
    }

    @Override
    public String threadName() {
        return String.format(
                "[Heavy] RelationshipImport (%d..%d)",
                nodeOffset,
                nodeOffset + nodeSize);
    }

    @Override
    public Void apply(final Statement statement) throws EntityNotFoundException {
        final ReadOperations readOp = statement.readOperations();
        final RelationshipLoader loader = prepare(readOp);
        PrimitiveIntIterator iterator = nodes.iterator();
        while (iterator.hasNext()) {
            final int nodeId = iterator.next();
            final long sourceNodeId = idMap.toOriginalNodeId(nodeId);
            loader.load(sourceNodeId, nodeId);
            progress.relProgress();
        }
        return null;
    }

    private RelationshipLoader prepare(final ReadOperations readOp) {
        final RelationshipLoader loader;
        if (setup.loadAsUndirected) {
            loader = prepareUndirected(readOp);
        } else {
            loader = prepareDirected(readOp);
        }

        if (this.nodeWeights instanceof WeightMap) {
            WeightMap nodeWeights = (WeightMap) this.nodeWeights;

            if (this.nodeProps instanceof WeightMap) {
                WeightMap nodeProps = (WeightMap) this.nodeProps;
                return new ReadWithNodeWeightsAndProps(loader, nodeWeights, nodeProps);
            }
            return new ReadWithNodeWeights(loader, nodeWeights);
        }
        if (this.nodeProps instanceof WeightMap) {
            WeightMap nodeProps = (WeightMap) this.nodeProps;
            return new ReadWithNodeWeights(loader, nodeProps);
        }

        return loader;
    }

    private RelationshipLoader prepareDirected(final ReadOperations readOp) {
        final boolean loadIncoming = setup.loadIncoming;
        final boolean loadOutgoing = setup.loadOutgoing;
        final boolean sort = setup.sort;
        final boolean shouldLoadWeights = relWeights instanceof WeightMap;

        RelationshipLoader loader = null;
        if (loadOutgoing) {
            final VisitRelationship visitor;
            if (shouldLoadWeights) {
                visitor = new VisitOutgoingWithWeight(readOp, idMap, sort, (WeightMap) this.relWeights);
            } else {
                visitor = new VisitOutgoingNoWeight(idMap, sort);
            }
            loader = new ReadOutgoing(readOp, matrix, relationId, visitor);
        }
        if (loadIncoming) {
            final VisitRelationship visitor;
            if (shouldLoadWeights) {
                visitor = new VisitIncomingWithWeight(readOp, idMap, sort, (WeightMap) this.relWeights);
            } else {
                visitor = new VisitIncomingNoWeight(idMap, sort);
            }
            if (loader != null) {
                ReadOutgoing readOutgoing = (ReadOutgoing) loader;
                loader = new ReadBoth(readOutgoing, visitor);
            } else {
                loader = new ReadIncoming(readOp, matrix, relationId, visitor);
            }
        }
        if (loader == null) {
            loader = new ReadNothing(readOp, matrix, relationId);
        }
        return loader;
    }

    private RelationshipLoader prepareUndirected(final ReadOperations readOp) {
        final VisitRelationship visitorIn;
        final VisitRelationship visitorOut;
        if (relWeights instanceof WeightMap) {
            visitorIn = new VisitIncomingNoWeight(idMap, true);
            visitorOut = new VisitUndirectedOutgoingWithWeight(readOp, idMap, true, (WeightMap) this.relWeights);
        } else {
            visitorIn = new VisitIncomingNoWeight(idMap, true);
            visitorOut = new VisitOutgoingNoWeight(idMap, true);
        }
        return new ReadUndirected(readOp, matrix, relationId, visitorOut, visitorIn);
    }

    Graph toGraph(final IdMap idMap, final AdjacencyMatrix matrix) {
        return new HeavyGraph(
                idMap,
                matrix,
                relWeights,
                nodeWeights,
                nodeProps);
    }

    void writeInto(
            WeightMapping relWeights,
            WeightMapping nodeWeights,
            WeightMapping nodeProps) {
        combineMaps(relWeights, this.relWeights);
        combineMaps(nodeWeights, this.nodeWeights);
        combineMaps(nodeProps, this.nodeProps);
    }

    void release() {
        this.idMap = null;
        this.matrix = null;
        this.relWeights = null;
        this.nodeWeights = null;
        this.nodeProps = null;
    }

    private void combineMaps(WeightMapping global, WeightMapping local) {
        if (global instanceof WeightMap && local instanceof WeightMap) {
            WeightMap localWeights = (WeightMap) local;
            final LongDoubleMap localMap = localWeights.weights();
            WeightMap globalWeights = (WeightMap) global;
            final LongDoubleMap globalMap = globalWeights.weights();

            for (LongDoubleCursor cursor : localMap) {
                globalMap.put(cursor.key, cursor.value);
            }
        }
    }
}
