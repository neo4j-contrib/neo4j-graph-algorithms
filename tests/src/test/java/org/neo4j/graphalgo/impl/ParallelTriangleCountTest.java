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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 *
 * @author mknblch
 */
public class ParallelTriangleCountTest {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    public static final int TRIANGLES = 10_000;

    private static GraphDatabaseAPI db;
    private static Graph graph;

    @BeforeClass
    public static void setup() throws Exception {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms"))) {

            final RelationshipType type = RelationshipType.withName(RELATIONSHIP);
            final DefaultBuilder builder = GraphBuilder.create(db)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newDefaultBuilder();
            final Node center = builder.createNode();
            builder.newRingBuilder()
                    .createRing(TRIANGLES)
                    .forEachNodeInTx(node -> {
                        center.createRelationshipTo(node, type);
                    });
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
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("eval took " + l + "ms"))) {
            final int triangleCount = new ParallelTriangleCount(graph, Pools.DEFAULT, 1)
                    .compute()
                    .getTriangleCount();
            assertEquals(TRIANGLES, triangleCount);
        }
    }

    @Test
    public void testParallel() throws Exception {
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("eval took " + l + "ms"))) {
            final int triangleCount = new ParallelTriangleCount(graph, Pools.DEFAULT, 8)
                    .compute()
                    .getTriangleCount();
            assertEquals(TRIANGLES, triangleCount);
        }
    }
}
