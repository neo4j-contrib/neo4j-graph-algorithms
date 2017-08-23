package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 *
 * @author mknblch
 */
public class TriangleCountTest {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    public static final long TRIANGLES = 1000L;

    private static GraphDatabaseAPI db;
    private static Graph graph;
    private static long centerId;

    @BeforeClass
    public static void setup() throws Exception {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms for " + TRIANGLES + " nodes"))) {

            final RelationshipType type = RelationshipType.withName(RELATIONSHIP);
            final DefaultBuilder builder = GraphBuilder.create(db)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newDefaultBuilder();
            final Node center = builder.createNode();
            builder.newRingBuilder()
                    .createRing((int)TRIANGLES)
                    .forEachNodeInTx(node -> {
                        center.createRelationshipTo(node, type);
                    });
            centerId = center.getId();
        };

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(db)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .load(HeavyGraphFactory.class);
        };
    }

    @Test
    public void testSequential() throws Exception {
        final TriangleCount algo = new TriangleCount(graph, Pools.DEFAULT, 1);
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("sequential count took " + l + "ms"))) {
            algo.compute();
        }
        assertEquals(TRIANGLES, (long) algo.getTriangleCount());
        assertTriangles(algo.getTriangles());
    }

    @Test
    public void testParallel() throws Exception {
        final TriangleCount algo = new TriangleCount(graph, Pools.DEFAULT, 4);
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("parallel count took " + l + "ms"))) {
            algo.compute();
        }
        assertEquals(TRIANGLES, (long) algo.getTriangleCount());
        assertTriangles(algo.getTriangles());
    }

    private void assertTriangles(AtomicIntegerArray triangles) {
        final int centerMapped = graph.toMappedNodeId(centerId);
        assertEquals(TRIANGLES, triangles.get(centerMapped));
        for (int i = 0; i < triangles.length(); i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(2, triangles.get(i));
        }
    }
}
