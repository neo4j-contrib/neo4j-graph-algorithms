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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.ExecutorService;


final class ScanningNodesImporter extends ScanningRecordsImporter<NodeRecord, HugeIdMap> {

    private final ImportProgress progress;
    private final AllocationTracker tracker;

    private HugeLongArrayBuilder idMapBuilder;

    ScanningNodesImporter(
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            ExecutorService threadPool,
            int concurrency) {
        super(NodeStoreScanner.NODE_ACCESS, "Node", api, dimensions, threadPool, concurrency);
        this.progress = progress;
        this.tracker = tracker;
    }

    @Override
    ImportingThreadPool.CreateScanner creator(
            long nodeCount,
            ImportSizing sizing,
            AbstractStorePageCacheScanner<NodeRecord> scanner) {
        idMapBuilder = HugeLongArrayBuilder.of(nodeCount, tracker);
        return NodesScanner.of(api, scanner, dimensions.labelId(), progress, idMapBuilder);
    }

    @Override
    HugeIdMap build() {
        return HugeIdMapBuilder.build(idMapBuilder, dimensions.allNodesCount(), tracker);
    }

}
