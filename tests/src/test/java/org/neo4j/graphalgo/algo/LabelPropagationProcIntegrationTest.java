package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.LabelPropagationProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class LabelPropagationProcIntegrationTest {

    private static GraphDatabaseAPI db;

    private static final String DB_CYPHER = "" +
            "CREATE (n {id: 0, partition: 1}) " +
            "CREATE (n)-[:X]->({id: 1, weight: 1.0, partition: 1}) " +
            "CREATE (n)-[:X]->({id: 2, weight: 2.0, partition: 1}) " +
            "CREATE (n)-[:X]->({id: 3, weight: 1.0, partition: 1}) " +
            "CREATE (n)-[:X]->({id: 4, weight: 1.0, partition: 1}) " +
            "CREATE (n)-[:X]->({id: 5, weight: 8.0, partition: 2})";

    @BeforeClass
    public static void setup() throws KernelException {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LabelPropagationProc.class);
    }

    @Test
    public void testLabelPropagation() throws Exception {
        String query = "CALL algo.labelPropagation(1,null,'partition','X','OUTGOING','weight')";
        String check = "MATCH (n) WHERE n.id = 0 RETURN n.partition AS partition";

        db.execute(query).close();
        try (Result result = db.execute(check)) {
            result.accept(row -> {
                assertEquals(row.getNumber("partition").intValue(), 2);
                return true;
            });
        }
    }
}
