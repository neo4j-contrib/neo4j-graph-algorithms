package org.neo4j.graphalgo.impl;

import algo.Pools;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.graphbuilder.GridBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.AllShortestPaths.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *     1
 *  (0)->(1)
 * 1 | 1 | 1
 *   v   v
 *  (2)->(3)
 * 1 | 1 | 1
 *   v   v
 *  (4)->(5)
 * 1 | 1 | 1
 *   v   v
 *  (6)->(7)
 * 1 | 1  | 1
 *   v    v
 *  (8)->(9)
 *
 * @author mknblch
 */
public class AllShortestPathsTest {

    private static final int width = 2, height = 5;

    private static final String PROPERTY = "property";
    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private static GraphDatabaseAPI db;
    private static GridBuilder gridBuilder;
    private static Graph graph;

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
                    .createGrid(width, height)
                    .forEachRelInTx(rel -> {
                        rel.setProperty(PROPERTY, 1.0);
                    });
        };

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(db)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withRelationshipWeightsFromProperty(PROPERTY, 1.0)
                    .load(HeavyGraphFactory.class);
        };
    }

    @Test
    public void testResults() throws Exception {

        final ResultConsumer mock = mock(ResultConsumer.class);

        new AllShortestPaths(graph, Pools.DEFAULT, 4)
                .resultStream()
                .peek(System.out::println)
                .forEach(r -> {
                    if (r.sourceNodeId > r.targetNodeId) {
                        assertEquals(Double.POSITIVE_INFINITY, r.distance, 0.1);
                    } else if (r.sourceNodeId == r.targetNodeId) {
                        assertEquals(0.0, r.distance, 0.1);
                    }
                    mock.test(r.sourceNodeId, r.targetNodeId, r.distance);
                });

        final int nodes = (width * height);

        verify(mock, times(nodes * nodes)).test(anyLong(), anyLong(), anyDouble());

        verify(mock, times(1)).test(0, 9, 5.0);
        verify(mock, times(1)).test(0, 0, 0.0);

    }

    interface ResultConsumer {

        void test(long source, long target, double distance);
    }
}
