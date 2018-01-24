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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
public class MultiStepColoringTest {

    public static final int NUM_SETS = 20;
    public static final int SET_SIZE = 1000;


    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static GraphDatabaseAPI db;

    private static Graph graph;

    private static ThreadToStatementContextBridge bridge;

    public static final String GRAPH_DIRECTORY = "/tmp/graph.db";

    private static File storeDir = new File(GRAPH_DIRECTORY);

    @BeforeClass
    public static void setup() throws Exception {

        FileUtils.deleteRecursively(storeDir);
        if (storeDir.exists()) {
            throw new IllegalStateException("could not delete " + storeDir);
        }

        if (null != db) {
            db.shutdown();
        }

        db = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(storeDir)
                .setConfig(GraphDatabaseSettings.pagecache_memory, "4G")
                .newGraphDatabase();

        bridge = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
            createTestGraph(NUM_SETS, SET_SIZE);
        }

        graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void shutdown() throws IOException {
        db.shutdown();

        FileUtils.deleteRecursively(storeDir);
        if (storeDir.exists()) {
            throw new IllegalStateException("Could not delete graph");
        }
    }


    private static void createTestGraph(int sets, int setSize) throws Exception {
        final int rIdx;
        try (Transaction tx = db.beginTx();
             Statement stm = bridge.get()) {
            rIdx = stm
                    .tokenWriteOperations()
                    .relationshipTypeGetOrCreateForName(RELATIONSHIP_TYPE.name());
            tx.success();
        }

        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < sets; i++) {
            runnables.add(createRing(setSize, rIdx));
        }
        ParallelUtil.run(runnables, Pools.DEFAULT);
    }

    private static Runnable createRing(int size, int rIdx) throws Exception {
        return () -> {
            try (Transaction tx = db.beginTx();
                 Statement stm = bridge.get()) {
                final DataWriteOperations op = stm.dataWriteOperations();
                long node = op.nodeCreate();
                long start = node;
                for (int i = 1; i < size; i++) {
                    final long temp = op.nodeCreate();
                    try {
                        op.relationshipCreate(rIdx, node, temp);
                    } catch (EntityNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    node = temp;
                }
                try {
                    // build circle
                    op.relationshipCreate(rIdx, node, start);
                } catch (EntityNotFoundException e) {
                    throw new RuntimeException(e);
                }
                tx.success();
            } catch (InvalidTransactionTypeKernelException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Test
    public void testMsColoring() throws Exception {

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("MSColoring took " + l + "ms"))) {
            final AtomicIntegerArray colors = new MSColoring(graph, Pools.DEFAULT, 8)
                    .compute()
                    .getColors();

            assertEquals(NUM_SETS, numColors(colors));
        }
    }

    private static int numColors(AtomicIntegerArray colors) {
        final IntIntMap map = new IntIntScatterMap();
        for (int i = colors.length() - 1; i >= 0; i--) {
            map.addTo(colors.get(i), 1);
        }
        return map.size();
    }
}
