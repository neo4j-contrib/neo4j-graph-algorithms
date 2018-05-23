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

import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ApproximatedImportProgress;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class HugeGraphFactory extends GraphFactory {

    public HugeGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public HugeGraph build() {
        return importGraph();
    }

    @Override
    protected ImportProgress importProgress(
            final ProgressLogger progressLogger,
            final GraphDimensions dimensions,
            final GraphSetup setup) {

        // ops for scanning degrees
        long relOperations = dimensions.maxRelCount();

        // batching for undirected double the amount of rels imported
        if (setup.loadIncoming || setup.loadAsUndirected) {
            relOperations += dimensions.maxRelCount();
        }
        if (setup.loadOutgoing || setup.loadAsUndirected) {
            relOperations += dimensions.maxRelCount();
        }

        return new ApproximatedImportProgress(
                progressLogger,
                setup.tracker,
                dimensions.hugeNodeCount(),
                relOperations
        );
    }

    private HugeGraph importGraph() {
        GraphDimensions dimensions = this.dimensions;
        log.info(
                "Importing up to %d nodes and up to %d relationships",
                dimensions.nodeCount(),
                dimensions.maxRelCount());
        int concurrency = setup.concurrency();
        AllocationTracker tracker = setup.tracker;
        HugeIdMap mapping = loadHugeIdMap(tracker);
        HugeGraph graph = loadRelationships(dimensions, tracker, mapping, concurrency);
        progressLogger.logDone(tracker);
        return graph;
    }

    private HugeGraph loadRelationships(
            GraphDimensions dimensions,
            AllocationTracker tracker,
            HugeIdMap mapping,
            int concurrency) {
        final long nodeCount = dimensions.hugeNodeCount();
        HugeAdjacencyBuilder outAdjacency = null;
        HugeAdjacencyBuilder inAdjacency = null;
        if (setup.loadAsUndirected) {
            outAdjacency = new HugeAdjacencyBuilder(nodeCount, tracker);
        } else {
            if (setup.loadOutgoing) {
                outAdjacency = new HugeAdjacencyBuilder(nodeCount, tracker);
            }
            if (setup.loadIncoming) {
                inAdjacency = new HugeAdjacencyBuilder(nodeCount, tracker);
            }
        }

        int weightProperty = dimensions.relWeightId();
        HugeWeightMapBuilder weightsBuilder = weightProperty == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new HugeWeightMapBuilder.NullBuilder(setup.relationDefaultWeight)
                : new HugeWeightMapBuilder(tracker, weightProperty, setup.relationDefaultWeight);

        final ScanningRelationshipImporter importer = ScanningRelationshipImporter.create(
                setup, api, progress, tracker, mapping, weightsBuilder,
                outAdjacency, inAdjacency, threadPool, concurrency);

        HugeWeightMapping weights = importer.run();
        return HugeAdjacencyBuilder.apply(tracker, mapping, weights, inAdjacency, outAdjacency);
    }

}
