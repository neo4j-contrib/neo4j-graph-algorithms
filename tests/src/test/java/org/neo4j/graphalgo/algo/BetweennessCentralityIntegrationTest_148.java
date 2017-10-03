package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 *
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class BetweennessCentralityIntegrationTest_148 {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(BetweennessCentralityProc.class);

        final String importQuery =
                "CREATE (nAlice:User {name:'Alice'})\n" +
                        ",(nBridget:User {name:'Bridget'})\n" +
                        ",(nCharles:User {name:'Charles'})\n" +
                        ",(nDoug:User {name:'Doug'})\n" +
                        ",(nMark:User {name:'Mark'})\n" +
                        "CREATE (nAlice)-[:FRIEND]->(nBridget)\n" +
                        ",(nCharles)-[:FRIEND]->(nBridget)\n" +
                        ",(nDoug)-[:FRIEND]->(nBridget)\n" +
                        ",(nMark)-[:FRIEND]->(nBridget)\n" +
                        ",(nMark)-[:FRIEND]->(nDoug)\n";

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.printf("Setup took %d ms%n", l))) {
            try (Transaction tx = db.beginTx()) {
                db.execute(importQuery);
                tx.success();
            }
        }

    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    private String name(long id) {
        String[] name = {""};
        db.execute("MATCH (n) WHERE id(n) = " + id + " RETURN n.name as name")
                .accept(row -> {
                    name[0] = row.getString("name");
                    return false;
                });
        if (name[0].isEmpty()) {
            throw new IllegalArgumentException("unknown id " + id);
        }
        return name[0];
    }

    @Test
    public void testBCStreamDirectionBoth() throws Exception {

        final Consumer mock = mock(Consumer.class);
        final String evalQuery = "CALL algo.betweenness.stream('User', 'FRIEND', {direction:'B'}) YIELD nodeId, centrality";
        db.execute(evalQuery).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final double centrality = row.getNumber("centrality").doubleValue();
            mock.consume(name(nodeId), centrality);
            return true;
        });

        verify(mock, times(4)).consume(anyString(), AdditionalMatchers.eq(0.0, 0.1));
        verify(mock, times(1)).consume(eq("Bridget"), AdditionalMatchers.eq(5.0, 0.1));
    }

    interface Consumer {
        void consume(String name, double centrality);
    }
}
