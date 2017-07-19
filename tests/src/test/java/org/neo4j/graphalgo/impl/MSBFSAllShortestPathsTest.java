package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * 1
 * (0)->(1)
 * 1 | 1 | 1
 * v   v
 * (2)->(3)
 * 1 | 1 | 1
 * v   v
 * (4)->(5)
 * 1 | 1 | 1
 * v   v
 * (6)->(7)
 * 1 | 1  | 1
 * v    v
 * (8)->(9)
 *
 * @author mknblch
 */
public class MSBFSAllShortestPathsTest {

    private static final int width = 2, height = 5;

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private static Graph graph;

    @BeforeClass
    public static void setup() throws Exception {

        final GraphDatabaseAPI db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer ignored = ProgressTimer.start(t -> System.out.println(
                "setup took " + t + "ms"))) {
            GraphBuilder.create(db)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newGridBuilder()
                    .createGrid(width, height);
        }

        try (ProgressTimer ignored = ProgressTimer.start(t -> System.out.println(
                "load took " + t + "ms"))) {
            graph = new GraphLoader(db)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .load(HeavyGraphFactory.class);
        }

        db.shutdown();
    }

    @Test
    public void testResults() throws Exception {

        final ResultConsumer mock = mock(ResultConsumer.class);

        new MSBFSAllShortestPaths(graph, Pools.DEFAULT)
                .resultStream()
                .peek(System.out::println)
                .forEach(r -> {
                    if (r.sourceNodeId > r.targetNodeId) {
                        fail("should not happen");
                    } else if (r.sourceNodeId == r.targetNodeId) {
                        fail("should not happen");
                    }
                    mock.test(r.sourceNodeId, r.targetNodeId, r.distance);
                });


        verify(mock, times(35)).test(anyLong(), anyLong(), anyDouble());
        verify(mock, times(1)).test(0, 9, 5.0);
    }

    interface ResultConsumer {

        void test(long source, long target, double distance);
    }
}
