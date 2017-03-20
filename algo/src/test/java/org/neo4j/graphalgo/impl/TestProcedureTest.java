package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.exceptions.KernelException;

/**
 * @author mknobloch
 */
public class TestProcedureTest extends Neo4JTestCase {


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
    public static void buildTestGraph() throws KernelException {

        registerProcedure(TestProcedure.class);

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
    public void smokeTest() throws Exception {
        db.execute("CALL graphalgo.helloworld");
    }

    @Test
    public void testPageRankKernel() throws Exception {
        final Result result = db.execute("CALL graphalgo.pagerank.kernel({label:'label', relation:'relation', iterations:100, dampingFactor:0.85})");
        while (result.hasNext()) {
            System.out.println(result.next());
        }
    }

    @Test
    public void testPageRankView() throws Exception {
        final Result result = db.execute("CALL graphalgo.pagerank.view({label:'label', relation:'relation', iterations:100, dampingFactor:0.85})");
        while (result.hasNext()) {
            System.out.println(result.next());
        }
    }

}