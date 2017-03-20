package org.neo4j.graphalgo;

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
public class PageRankGraphTest extends Neo4JTestCase {

    private static final Logger LOGGER = getLogger(PageRankGraphTest.class);
    private static int nodeA;
    private static int nodeB;
    private static int nodeC;
    private static int nodeD;
    private static int nodeE;
    private static int nodeF;
    private static int nodeG;
    private static int nodeH;
    private static int nodeI;
    private static int nodeJ;
    private static int nodeK;

    @BeforeClass
    public static void buildTestGraph() {

        nodeA = newNode();
        nodeB = newNode();
        nodeC = newNode();
        nodeD = newNode();
        nodeE = newNode();
        nodeF = newNode();
        nodeG = newNode();
        nodeH = newNode();
        nodeI = newNode();
        nodeJ = newNode();
        nodeK = newNode();

        newRelation(nodeB, nodeC);

        newRelation(nodeC, nodeB);

        newRelation(nodeD, nodeA);
        newRelation(nodeD, nodeB);

        newRelation(nodeE, nodeB);
        newRelation(nodeE, nodeD);
        newRelation(nodeE, nodeF);

        newRelation(nodeF, nodeB);
        newRelation(nodeF, nodeE);

        newRelation(nodeG, nodeB);
        newRelation(nodeG, nodeE);

        newRelation(nodeH, nodeB);
        newRelation(nodeH, nodeE);

        newRelation(nodeI, nodeB);
        newRelation(nodeI, nodeE);

        newRelation(nodeJ, nodeE);

        newRelation(nodeK, nodeE);
    }

    @Test
    public void testLightGraph() throws Exception {
        testPageRank(lightGraph());
    }

    @Test
    public void testHeavyGraph() throws Exception {
        testPageRank(heavyGraph());
    }

    @Test
    public void testView() throws Exception {
        testPageRank(neo4jView());
    }

    private void testPageRank(Graph graph) {
        // Act
        final double[] ranks = new PageRankKernel(graph, 0.85).compute(100);

        for (int i = 0; i < ranks.length; i++) {
            double rank = ranks[i];
            LOGGER.info("{}:{}", i, rank);
        }

        assertEquals(0.32, ranks[graph.toMappedNodeId(nodeB)], 10E-3);
        assertEquals(0.28, ranks[graph.toMappedNodeId(nodeC)], 10E-3);
        assertEquals(0.032, ranks[graph.toMappedNodeId(nodeD)], 10E-4);
        assertEquals(0.013, ranks[graph.toMappedNodeId(nodeG)], 10E-4);
    }
}
