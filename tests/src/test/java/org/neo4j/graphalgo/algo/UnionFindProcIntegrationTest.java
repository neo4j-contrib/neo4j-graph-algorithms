package org.neo4j.graphalgo.algo;

import algo.algo.PageRank;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.UnionFindProc;
import org.neo4j.graphalgo.core.sources.BufferedAllRelationshipIterator;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.sources.SingleRunAllRelationIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * @author mknblch
 */
public class UnionFindProcIntegrationTest {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph = "CREATE (nA)\n" +
                "CREATE (nB)\n" +
                "CREATE (nC)\n" +
                "CREATE (nD)\n" +
                "CREATE (nE)\n" +
                "CREATE (nF)\n" +
                "CREATE (nG)\n" +
                "CREATE (nH)\n" +
                "CREATE (nI)\n" +
                "CREATE (nJ)\n" +
                "CREATE (nK)\n" +
                "CREATE\n" +
                "  (nA)-[:TYPE]->(nB),\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nD),\n" +
                "  (nD)-[:TYPE]->(nA),\n" +

                "  (nE)-[:TYPE]->(nF),\n" +
                "  (nF)-[:TYPE]->(nG),\n" +
                "  (nG)-[:TYPE]->(nH),\n" +
                "  (nH)-[:TYPE]->(nI),\n" +
                "  (nI)-[:TYPE]->(nE),\n" +

                "  (nJ)-[:TYPE]->(nK),\n" +
                "  (nK)-[:TYPE]->(nJ);";

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

        Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);

        proceduresService.registerProcedure(UnionFindProc.class);
    }

    @Test
    public void test() throws Exception {

        db.execute("CALL org.neo4j.graphalgo.unionFind() YIELD nodeId, setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    System.out.println(row.get("nodeId") + " -> " + row.get("setId"));
                    return true;
                });
    }
}
