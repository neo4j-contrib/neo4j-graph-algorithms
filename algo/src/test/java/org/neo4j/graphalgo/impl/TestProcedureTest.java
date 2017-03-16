package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.exceptions.KernelException;

/**
 * @author mknobloch
 */
public class TestProcedureTest extends Neo4JTestCase {


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
    public static void buildTestGraph() throws KernelException {

        registerProcedure(TestProcedure.class);

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