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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class HugeGraphFactory extends GraphFactory {

    public HugeGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public HugeGraph build() {
        return importGraph();
    }

    private HugeGraph importGraph() {
        GraphDimensions dimensions = this.dimensions;
        log.info(
                "Importing up to %d nodes and up to %d relationships",
                dimensions.nodeCount(),
                dimensions.maxRelCount());
        int concurrency = setup.concurrency();
        AllocationTracker tracker = setup.tracker;
        HugeWeightMapping weights = hugeWeightMapping(tracker, dimensions.relWeightId(), setup.relationDefaultWeight);
        HugeIdMap mapping = loadHugeIdMap(tracker);
        HugeGraph graph = loadRelationships(dimensions, tracker, mapping, weights, concurrency);
        progressLogger.logDone(tracker);
        return graph;
    }

    private HugeGraph loadRelationships(
            GraphDimensions dimensions,
            AllocationTracker tracker,
            HugeIdMap mapping,
            HugeWeightMapping weights,
            int concurrency) {
        final long nodeCount = dimensions.hugeNodeCount();
        HugeLongArray outOffsets = null;
        HugeAdjacencyListBuilder outAdjacency = null;
        HugeLongArray inOffsets = null;
        HugeAdjacencyListBuilder inAdjacency = null;
        if (setup.loadAsUndirected) {
            outOffsets = HugeLongArray.newArray(nodeCount, tracker);
            outAdjacency = HugeAdjacencyListBuilder.newBuilder(tracker);
        } else {
            if (setup.loadOutgoing) {
                outOffsets = HugeLongArray.newArray(nodeCount, tracker);
                outAdjacency = HugeAdjacencyListBuilder.newBuilder(tracker);
            }
            if (setup.loadIncoming) {
                inOffsets = HugeLongArray.newArray(nodeCount, tracker);
                inAdjacency = HugeAdjacencyListBuilder.newBuilder(tracker);
            }
        }

        final ScanningRelationshipImporter importer = ScanningRelationshipImporter.create(
                setup, api, progress, mapping,
                outOffsets, outAdjacency, inOffsets, inAdjacency,
                threadPool, concurrency);
        if (importer != null) {
            importer.run();
        }

        return new HugeGraphImpl(
                tracker,
                mapping,
                weights,
                inAdjacency != null ? inAdjacency.build() : null,
                outAdjacency != null ? outAdjacency.build() : null,
                inOffsets,
                outOffsets
        );
    }

}
