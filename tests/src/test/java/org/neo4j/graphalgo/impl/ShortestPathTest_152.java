package org.neo4j.graphalgo.impl;

import algo.algo.PathFinding;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.ShortestPathDeltaSteppingProc;
import org.neo4j.graphalgo.ShortestPathProc;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.function.DoubleConsumer;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
public class ShortestPathTest_152 {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraph() throws KernelException {
        String cypher = "CREATE (a:Loc{name:'A'}), " +
                "(b:Loc{name:'B'}), " +
                "(c:Loc{name:'C'}), " +
                "(d:Loc{name:'D'}), " +
                "(e:Loc{name:'E'}), " +
                "(f:Loc{name:'F'}),\n" +
                " (a)-[:ROAD {d:50}]->(b),\n" +
                " (a)-[:ROAD {d:50}]->(c),\n" +
                " (a)-[:ROAD {d:100}]->(d),\n" +
                " (a)-[:RAIL {d:50}]->(d),\n" +
                " (b)-[:ROAD {d:40}]->(d),\n" +
                " (c)-[:ROAD {d:40}]->(d),\n" +
                " (c)-[:ROAD {d:80}]->(e),\n" +
                " (d)-[:ROAD {d:30}]->(e),\n" +
                " (d)-[:ROAD {d:80}]->(f),\n" +
                " (e)-[:ROAD {d:40}]->(f),\n" +
                " (e)-[:RAIL {d:20}]->(f);";

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(ShortestPathProc.class);

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        db.shutdown();
    }

    @Test
    public void testDijkstraProcedure() throws Exception {

        DoubleConsumer mock = mock(DoubleConsumer.class);

        String cypher = "MATCH (from:Loc{name:'A'}), (to:Loc{name:'F'}) " +
                "CALL algo.shortestPath.stream(from, to, 'd', {relationshipQuery:'ROAD', defaultValue:999999.0}) " +
                "YIELD nodeId, cost with nodeId, cost MATCH(n) WHERE id(n) = nodeId RETURN n.name as name, cost;";

        db.execute(cypher).accept(row -> {
            System.out.println(row.get("name") + ":" + row.get("cost"));
            mock.accept(row.getNumber("cost").doubleValue());
            return true;
        });

        verify(mock, times(1)).accept(eq(0.0));
        verify(mock, times(1)).accept(eq(50.0));
        verify(mock, times(1)).accept(eq(90.0));
        verify(mock, times(1)).accept(eq(120.0));
        verify(mock, times(1)).accept(eq(160.0));
    }
}
