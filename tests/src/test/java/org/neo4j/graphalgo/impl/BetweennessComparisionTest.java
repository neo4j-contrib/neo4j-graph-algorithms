package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
@Ignore("playground")
public class BetweennessComparisionTest {

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    public static final String TYPE = "TYPE";
    public static final int NODE_COUNT = 500;
    private static Graph graph;
    private static int centerId;
    private static double[] expected;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        long cId;
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("build took " + l + "ms"))) {

            final DefaultBuilder defaultBuilder = GraphBuilder.create(DB)
                    .setLabel("Node")
                    .setRelationship(TYPE);

            final Node center = defaultBuilder.createNode();

            defaultBuilder
                    .newCompleteGraphBuilder()
                    .createCompleteGraph(NODE_COUNT - 1)
                    .forEachRelInTx(r -> {
                        if (Math.random() > 0.005) {
                            r.delete();
                        }
                    })
                    .forEachNodeInTx(n -> {
                        defaultBuilder.createRelationship(n, center);
                    });

            cId = center.getId();
        }

        graph = new GraphLoader(DB)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withoutRelationshipWeights()
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withDirection(Direction.OUTGOING)
                .load(HeavyGraphFactory.class);

        centerId = graph.toMappedNodeId(cId);

        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("reference bc took " + l + "ms"))) {
            expected = new BetweennessCentrality(graph)
                    .compute()
                    .getCentrality();
        };
    }

    @Test
    public void testMaxDepth() throws Exception {
        for (int d = 2; d < 5; d++) {
            final double[] centrality = new MaxDepthBetweennessCentrality(graph, d)
                    .compute()
                    .getCentrality();
            assertLowError(centrality);
        }
    }

    @Test
    public void test() throws Exception {

        final double[] centrality = new RABrandesBetweennessCentrality(graph, Pools.DEFAULT, 1)
                .compute()
                .getCentrality()
                .toArray();

        assertLowError(centrality);
    }


    @Test
    public void testParallel() throws Exception {

        final double[] centrality = new ParallelBetweennessCentrality(graph, Pools.DEFAULT, 4)
                .compute()
                .getCentrality()
                .toArray();

        assertLowError(centrality);
    }

    public void assertLowError(double[] centrality) {
        final Error e = new Error(centrality);
        System.out.println("maxError = " + e.max);
        System.out.println("meanError = " + e.mean());
        System.out.println("centerError = " + e.e[centerId]);
//        assertEquals("maximum error exceeds 30%", 0.0, e.max, 0.3);
        assertEquals("mean error exceeds 20%", 0.0, e.mean(), 0.2);
        assertEquals("error at most important node exceeded 10%", 0.0, e.e[centerId], 0.1);
    }

    private class Error {

        private double[] centrality;
        double[] e = new double[NODE_COUNT];
        double[] abs = new double[NODE_COUNT];

        double max = 0.0;
        double sum = 0.0;
        double absE = 0.0;

        public Error(double[] centrality) {
            this.centrality = centrality;
            for (int i = 0; i < NODE_COUNT; i++) {
                abs[i] = Math.abs(expected[i] - centrality[i]);
                if (i > 0) {
                    absE += Math.abs(abs[i] - abs[i - 1]);
                }
                e[i] = abs[i] == 0.0 ? 0.0 : abs[i] / expected[i];
                max = Math.max(e[i], max);
                sum += e[i];
            }
        }

        public void print() {
            int skipped = 0;
            for (int i = 0; i < NODE_COUNT; i++) {
                if (e[i] > 0.01) {
                    System.out.println(i + " " + expected[i] + " " + centrality[i] + " " + e[i]);
                } else {
                    skipped++;
                }
            }
            if (skipped > 0) {
                System.out.println(skipped + " nodes have an error <= 0.01");
            }
        }

        public double mean() {
            return sum / NODE_COUNT;
        }

        public double rel() {
            return absE / NODE_COUNT;
        }

    }
}
