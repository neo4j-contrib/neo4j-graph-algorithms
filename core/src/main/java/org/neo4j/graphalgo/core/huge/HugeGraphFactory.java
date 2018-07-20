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
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class HugeGraphFactory extends GraphFactory {

    // TODO: make this configurable from somewhere
    private static final boolean LOAD_DEGREES = true;

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
        long relOperations = LOAD_DEGREES ? dimensions.maxRelCount() : 0L;

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
        HugeAdjacencyBuilder outAdjacency = null;
        HugeAdjacencyBuilder inAdjacency = null;
        if (setup.loadAsUndirected) {
            outAdjacency = new HugeAdjacencyBuilder(tracker);
        } else {
            if (setup.loadOutgoing) {
                outAdjacency = new HugeAdjacencyBuilder(tracker);
            }
            if (setup.loadIncoming) {
                inAdjacency = new HugeAdjacencyBuilder(tracker);
            }
        }

        int weightProperty = dimensions.relWeightId();
        HugeWeightMapBuilder weightsBuilder = weightProperty == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new HugeWeightMapBuilder.NullBuilder(setup.relationDefaultWeight)
                : new HugeWeightMapBuilder(tracker, weightProperty, setup.relationDefaultWeight);

        final Runnable importer = determineImportStrategy(
                dimensions, tracker, mapping, weightsBuilder,
                outAdjacency, inAdjacency, concurrency
        );

        importer.run();
        HugeWeightMapping weights = weightsBuilder.build();
        return HugeAdjacencyBuilder.apply(tracker, mapping, weights, inAdjacency, outAdjacency);
    }

    private Runnable determineImportStrategy(
            GraphDimensions dimensions,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeWeightMapBuilder weights,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            int concurrency) {
        long pageCacheOversizing = getPageCacheOversizing();
        if (pageCacheOversizing > 0L) {
            return new NodesBasedImporter(
                    setup, api, dimensions, progress, tracker, idMap, weights,
                    outAdjacency, inAdjacency, threadPool, LOAD_DEGREES, concurrency);
        } else {
            return ScanningRelationshipImporter.create(
                    setup, api, dimensions, progress, tracker, idMap, weights,
                    outAdjacency, inAdjacency, threadPool, LOAD_DEGREES, concurrency);
        }
    }

    private long getPageCacheOversizing() {
        try {
            DependencyResolver dep = api.getDependencyResolver();
            Config config = dep.resolveDependency(Config.class);
            String mem = config.get(GraphDatabaseSettings.pagecache_memory);
            long maxPageCacheAvailable = ByteUnit.parse(mem);

            RecordStorageEngine storageEngine = dep.resolveDependency(RecordStorageEngine.class);
            NeoStores neoStores = storageEngine.testAccessNeoStores();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            long relsInUse = relationshipStore.getNumberOfIdsInUse();
            int recordsPerPage = relationshipStore.getRecordsPerPage();
            long idsInPages = align(relsInUse, (long) recordsPerPage);
            long requiredBytes = idsInPages * (long) relationshipStore.getRecordDataSize();

            return maxPageCacheAvailable - requiredBytes;
        } catch (Exception e) {
            log.warn("Could not determine sizes of page cache and relationship store", e);
            // pretend that the page cache and relationship store are perfectly balanced, as all things should be
            return 0L;
        }
    }

    private static long align(long value, long boundary) {
        return ((value + (boundary - 1L)) / boundary) * boundary;
    }
}
