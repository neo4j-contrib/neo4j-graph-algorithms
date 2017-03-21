package org.neo4j.graphalgo.core.heavyweight;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphdb.Direction;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.*;

/**
 * @author mknobloch
 */
@RunWith(MockitoJUnitRunner.class)
public class AdjacencyMatrixTest {

    private static AdjacencyMatrix matrix;



    @BeforeClass
    public static void setup() {
        matrix = new AdjacencyMatrix(3);

        // 0 -> {1, 2}
        matrix.armOut(0, 2);
        matrix.addOutgoing(0, 1, 0);
        matrix.addOutgoing(0, 2, 1);

        // 1 -> {2}
        matrix.armOut(1, 1);
        matrix.addOutgoing(1, 2, 2);

        // 2 -> {}
        matrix.armOut(2, 0);

        // 0 <- {}
        matrix.armIn(0, 0);

        // 1 <- {0}
        matrix.armIn(1, 1);
        matrix.addIncoming(0, 1, 0);

        // 2 <- {0, 1}
        matrix.armIn(2, 2);
        matrix.addIncoming(0, 2, 1);
        matrix.addIncoming(1, 2, 2);
    }

    @Test
    public void testOutgoingDegree() throws Exception {
        assertEquals(2, matrix.degree(0, OUTGOING));
        assertEquals(1, matrix.degree(1, OUTGOING));
        assertEquals(0, matrix.degree(2, OUTGOING));
    }

    @Test
    public void testIncomingDegree() throws Exception {
        assertEquals(0, matrix.degree(0, INCOMING));
        assertEquals(1, matrix.degree(1, INCOMING));
        assertEquals(2, matrix.degree(2, INCOMING));
    }

    @Test
    public void test() throws Exception {

    }
}
