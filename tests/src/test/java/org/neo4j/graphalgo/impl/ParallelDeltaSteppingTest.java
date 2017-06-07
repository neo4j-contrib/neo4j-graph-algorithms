package org.neo4j.graphalgo.impl;

import algo.Pools;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.graphbuilder.GridBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.Executors;

/**
 * The test creates a grid of nodes and computes a reference array
 * of shortest paths using one thread. It then compares the reference
 * against the result of several parallel computations to provoke
 * concurrency errors if any.
 *
 * @author mknblch
 */
public class ParallelDeltaSteppingTest {

    private static final String PROPERTY = "property";
    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private static GraphDatabaseAPI db;
    private static GridBuilder gridBuilder;
    private static Graph graph;
    private static double[] reference;
    private static long rootNodeId;

    @BeforeClass
    public static void setup() throws Exception {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms"))) {
            gridBuilder = GraphBuilder.create(db)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newGridBuilder()
                    .createGrid(100, 100)
                    .forEachRelInTx(rel -> {
                        rel.setProperty(PROPERTY, Math.random() * 5); // (0-5)
                    });

            rootNodeId = gridBuilder.getLineNodes()
                    .get(0)
                    .get(0)
                    .getId();
        };

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(db)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withRelationshipWeightsFromProperty(PROPERTY, 1.0)
                    .load(HeavyGraphFactory.class);
        };

        reference = compute(1);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testParallelBehaviour() throws Exception {
        final int n = 20;
        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println(n + "x eval took " + t + "ms"))) {
            for (int i = 0; i < n; i++) {
                Assert.assertArrayEquals("error in iteration " + i,
                        reference,
                        compute((n % 7) + 2),
                        0.001);
            }
        }
    }

    private static double[] compute(int threads) throws Exception {
        return new ShortestPathDeltaStepping(graph, 2.5)
                .withExecutorService(Executors.newFixedThreadPool(threads))
                .compute(rootNodeId)
                .getShortestPaths();
    }
}
