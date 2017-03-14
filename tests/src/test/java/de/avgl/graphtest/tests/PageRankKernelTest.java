package de.avgl.graphtest.tests;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.PageRankKernel;
import org.slf4j.Logger;

import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;


/**
 * example taken from https://de.wikipedia.org/wiki/PageRank
 *
 * @author mknobloch
 */
public class PageRankKernelTest extends Neo4JTestCase {

    private static final Logger LOGGER = getLogger(PageRankKernelTest.class);
    private static int vertexA;
    private static int vertexB;
    private static int vertexC;
    private static int vertexD;
    private static int vertexE;
    private static int vertexF;
    private static int vertexG;
    private static int vertexH;
    private static int vertexI;
    private static int vertexJ;
    private static int vertexK;

    @BeforeClass
    public static void buildTestGraph() {

        vertexA = newVertex();
        vertexB = newVertex();
        vertexC = newVertex();
        vertexD = newVertex();
        vertexE = newVertex();
        vertexF = newVertex();
        vertexG = newVertex();
        vertexH = newVertex();
        vertexI = newVertex();
        vertexJ = newVertex();
        vertexK = newVertex();

        newEdge(vertexB, vertexC);

        newEdge(vertexC, vertexB);

        newEdge(vertexD, vertexA);
        newEdge(vertexD, vertexB);

        newEdge(vertexE, vertexB);
        newEdge(vertexE, vertexD);
        newEdge(vertexE, vertexF);

        newEdge(vertexF, vertexB);
        newEdge(vertexF, vertexE);

        newEdge(vertexG, vertexB);
        newEdge(vertexG, vertexE);

        newEdge(vertexH, vertexB);
        newEdge(vertexH, vertexE);

        newEdge(vertexI, vertexB);
        newEdge(vertexI, vertexE);

        newEdge(vertexJ, vertexE);

        newEdge(vertexK, vertexE);
    }

    @Test
    public void testPageRankKernel() {
        // Act
        Graph graph = heavyGraph();
        final double[] ranks = new PageRankKernel(graph, 0.85).compute(100);

        for (int i = 0; i < ranks.length; i++) {
            double rank = ranks[i];
            LOGGER.info("{}:{}", i, rank);
        }

        assertEquals(0.32, ranks[graph.toVertexId(vertexB)], 10E-3);
        assertEquals(0.28, ranks[graph.toVertexId(vertexC)], 10E-3);
        assertEquals(0.032, ranks[graph.toVertexId(vertexD)], 10E-4);
        assertEquals(0.013, ranks[graph.toVertexId(vertexG)], 10E-4);
    }
}
