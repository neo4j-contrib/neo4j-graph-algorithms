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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * @author mknblch
 */
public class HeavyGraphFactory extends GraphFactory {

    public HeavyGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public Graph build() {
        return build(setup.batchSize);
    }

    /* test-private */ Graph build(int batchSize) {
        try {
            return importGraph(batchSize);
        } catch (EntityNotFoundException e) {
            throw Exceptions.launderedException(e);
        }
    }

    private Graph importGraph(final int batchSize) throws
            EntityNotFoundException {
        final IdMap idMap = loadIdMap();

        final Supplier<WeightMapping> relWeights = () -> newWeightMap(
                dimensions.relWeightId(),
                setup.relationDefaultWeight);
        final Supplier<WeightMapping> nodeWeights = () -> newWeightMap(
                dimensions.nodeWeightId(),
                setup.nodeDefaultWeight);
        final Supplier<WeightMapping> nodeProps = () -> newWeightMap(
                dimensions.nodePropId(),
                setup.nodeDefaultPropertyValue);

        int concurrency = setup.concurrency();
        final int nodeCount = dimensions.nodeCount();
        final AdjacencyMatrix matrix = new AdjacencyMatrix(
                nodeCount,
                setup.loadIncoming && !setup.loadAsUndirected,
                setup.loadOutgoing || setup.loadAsUndirected,
                setup.sort || setup.loadAsUndirected,
                false);
        int actualBatchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                batchSize);
        Collection<RelationshipImporter> tasks = ParallelUtil.readParallel(
                concurrency,
                actualBatchSize,
                idMap,
                (offset, nodeIds) -> new RelationshipImporter(
                        api,
                        setup,
                        dimensions,
                        progress,
                        actualBatchSize,
                        offset,
                        idMap,
                        matrix,
                        nodeIds,
                        relWeights,
                        nodeWeights,
                        nodeProps
                ),
                threadPool);

        final Graph graph = buildCompleteGraph(
                matrix,
                idMap,
                relWeights,
                nodeWeights,
                nodeProps,
                tasks);

        progressLogger.logDone();
        return graph;
    }

    private Graph buildCompleteGraph(
            final AdjacencyMatrix matrix,
            final IdMap idMap,
            final Supplier<WeightMapping> relWeightsSupplier,
            final Supplier<WeightMapping> nodeWeightsSupplier,
            final Supplier<WeightMapping> nodePropsSupplier,
            Collection<RelationshipImporter> tasks) {
        if (tasks.size() == 1) {
            RelationshipImporter importer = tasks.iterator().next();
            final Graph graph = importer.toGraph(idMap, matrix);
            importer.release();
            return graph;
        }

        final WeightMapping relWeights = relWeightsSupplier.get();
        final WeightMapping nodeWeights = nodeWeightsSupplier.get();
        final WeightMapping nodeProps = nodePropsSupplier.get();
        for (RelationshipImporter task : tasks) {
            task.writeInto(relWeights, nodeWeights, nodeProps);
            task.release();
        }

        return new HeavyGraph(
                idMap,
                matrix,
                relWeights,
                nodeWeights,
                nodeProps);
    }
}
