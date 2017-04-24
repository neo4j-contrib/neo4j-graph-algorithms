package org.neo4j.graphalgo.algo;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.DijkstraProc;
import org.neo4j.graphalgo.UnionFindProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
public class ShortestPathIntegrationTest {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph =
                "CREATE (nA:Node{type:'start'})\n" + // start
                "CREATE (nB:Node)\n" +
                "CREATE (nC:Node)\n" +
                "CREATE (nD:Node)\n" +
                "CREATE (nX:Node{type:'end'})\n" + // end
                "CREATE\n" +

                // sum: 5.0
                "  (nA)-[:TYPE {cost:5.0}]->(nX),\n" +
                // sum: 4.0
                "  (nA)-[:TYPE {cost:2.0}]->(nB),\n" +
                "  (nB)-[:TYPE {cost:2.0}]->(nX),\n" +
                // sum: 3.0
                "  (nA)-[:TYPE {cost:1.0}]->(nC),\n" +
                "  (nC)-[:TYPE {cost:1.0}]->(nD),\n" +
                "  (nD)-[:TYPE {cost:1.0}]->(nX)";


        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(DijkstraProc.class);
    }

    @Test
    public void testDijkstraStream() throws Exception {
        PathConsumer consumer = mock(PathConsumer.class);
        db.execute(
                "MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.dijkstraStream(start, end, 'cost') YIELD nodeId, cost\n" +
                        "RETURN nodeId, cost")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.accept((Long) row.getNumber("nodeId"), (Double) row.getNumber("cost"));
                    return true;
                });
        verify(consumer, times(4)).accept(anyLong(), anyDouble());
        verify(consumer, times(1)).accept(anyLong(), eq(0.0));
        verify(consumer, times(1)).accept(anyLong(), eq(1.0));
        verify(consumer, times(1)).accept(anyLong(), eq(2.0));
        verify(consumer, times(1)).accept(anyLong(), eq(3.0));
    }


    @Test
    public void testDijkstra() throws Exception {
        db.execute(
                "MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.dijkstra(start, end, 'cost') YIELD loadDuration, evalDuration, nodeCount, totalCost\n" +
                        "RETURN loadDuration, evalDuration, nodeCount, totalCost")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3.0, (Double) row.getNumber("totalCost"), 10E2);
                    assertEquals(4L, row.getNumber("nodeCount"));
                    assertNotEquals(-1L, row.getNumber("loadDuration"));
                    assertNotEquals(-1L, row.getNumber("evalDuration"));
                    return false;
                });
    }

    private interface PathConsumer {
        void accept(long nodeId, double cost);
    }

}
