package org.neo4j.graphalgo;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.ShortestPathDijkstraKernel;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;


/**
 * (0) - 1 -> (1) - 2 -> (2) - 3 -> (4)
 * (0) - 4 -> (3) - 3 -> (4)
 *
 * @author mknobloch
 */
// TODO broken
public class ShortestPathDijkstraKernelTest extends Neo4JTestCase {

    private static final Logger LOGGER = getLogger(ShortestPathDijkstraKernelTest.class);
    private static int v0;
    private static int v1;
    private static int v2;
    private static int v3;
    private static int v4;
    private static int v5;

    @BeforeClass
    public static void buildTestGraph() {

        v0 = newNode();
        v1 = newNode();
        v2 = newNode();
        v3 = newNode();
        v4 = newNode();
        v5 = newNode();

        newRelation(v0, v1, 1.0);
        newRelation(v1, v2, 1.0);
        newRelation(v2, v4, 1.0);

        newRelation(v0, v3, 1.0);
        newRelation(v3, v4, 1.0);
    }

    @Ignore("BufferAllocationException")
    @Test
    public void testShortestPathKernel() {

        Graph graph = lightGraph();
        int[] path = new ShortestPathDijkstraKernel(graph).compute(
                graph.toMappedNodeId(v0),
                graph.toMappedNodeId(v4));

        for (int i : path) {
            System.out.println(graph.toOriginalNodeId(i));
        }
        LOGGER.info("{}", path);
//        assertArrayEquals(new int[]{
//                graph.graphId(v0.id()),
//                graph.graphId(v1.id()),
//                graph.graphId(v2.id()),
//                graph.graphId(v4.id())
//        }, path);
    }
}
