package de.avgl.graphtest.tests;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.WeightedGraph;
import org.neo4j.graphalgo.impl.ShortestPathDijkstraKernel;
import org.neo4j.graphdb.Node;
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

        v0 = newVertex();
        v1 = newVertex();
        v2 = newVertex();
        v3 = newVertex();
        v4 = newVertex();
        v5 = newVertex();

        newEdge(v0, v1, 1.0);
        newEdge(v1, v2, 1.0);
        newEdge(v2, v4, 1.0);

        newEdge(v0, v3, 1.0);
        newEdge(v3, v4, 1.0);
    }

    @Test
    public void testShortestPathKernel() {

        WeightedGraph graph = lightGraph();
        int[] path = new ShortestPathDijkstraKernel(graph).compute(
                graph.toVertexId(v0),
                graph.toVertexId(v5));

        for (int i : path) {
            System.out.println(graph.toNodeId(i));
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
