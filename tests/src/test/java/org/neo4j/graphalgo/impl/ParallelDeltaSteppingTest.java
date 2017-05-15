package org.neo4j.graphalgo.impl;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.graphbuilder.GridBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.Executors;

/**
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

        gridBuilder = GraphBuilder.create(db)
                .setLabel(LABEL)
                .setRelationship(RELATIONSHIP)
                .newGridBuilder()
                .createGrid(10, 10)
                .forEachRelInTx(rel -> {
                    rel.setProperty(PROPERTY, Math.random() * 5); // [0 - 5]
                });

        rootNodeId = gridBuilder.getLineNodes().get(0).get(0).getId();

        graph = new GraphLoader(db)
                .withLabel(LABEL)
                .withRelationshipType(RELATIONSHIP)
                .withRelationshipWeightsFromProperty(PROPERTY, 1.0)
                .load(HeavyGraphFactory.class);

        reference = compute(1);
    }

    public static double[] compute(int threads) throws Exception {
        double[] shortestPaths = new ShortestPathDeltaStepping(graph, 2.5)
                .withExecutorService(Executors.newFixedThreadPool(threads))
                .compute(rootNodeId)
                .getShortestPaths();
        // copy bc distance array gets overwritten in every computation
        double[] paths = new double[shortestPaths.length];
        System.arraycopy(shortestPaths, 0, paths, 0, shortestPaths.length);
        return paths;
    }

    @Test
    public void testParallelBehaviour() throws Exception {
        for (int i = 0; i < 10; i++) {
            Assert.assertArrayEquals("error in iteration " + i, reference, compute(3), 0.001);
        }
    }
}
