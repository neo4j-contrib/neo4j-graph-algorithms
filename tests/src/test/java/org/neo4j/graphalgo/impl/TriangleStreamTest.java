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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.intThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author mknblch
 */
public class TriangleStreamTest {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    public static final long TRIANGLES = 1000;

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

        final TripleConsumer mock = mock(TripleConsumer.class);

        new TriangleStream(graph, Pools.DEFAULT, 1)
                .resultStream()
                .forEach(r -> mock.consume(r.nodeA, r.nodeB, r.nodeC));

        verify(mock, times((int)TRIANGLES)).consume(eq(centerId), anyLong(), anyLong());
    }

    @Test
    public void testParallel() throws Exception {

        final TripleConsumer mock = mock(TripleConsumer.class);

        new TriangleStream(graph, Pools.DEFAULT, 8)
                .resultStream()
                .forEach(r -> mock.consume(r.nodeA, r.nodeB, r.nodeC));

        verify(mock, times((int)TRIANGLES)).consume(eq(centerId), anyLong(), anyLong());
    }

    interface TripleConsumer {
        void consume(long nodeA, long nodeB, long nodeC);
    }

}
