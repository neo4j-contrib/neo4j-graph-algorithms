package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.KShortestPathsProc;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;

/**
 * Graph:
 *
 *            (0)
 *          /  |  \
 *       (4)--(5)--(1)
 *         \  /  \ /
 *         (3)---(2)
 *
 * @author mknblch
 */
public class YensKShortestPathsTest {

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE {cost:1.0}]->(b),\n" +
                        " (b)-[:TYPE {cost:1.0}]->(c),\n" +
                        " (c)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (e)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (a)-[:TYPE {cost:1.0}]->(e),\n" +

                        " (a)-[:TYPE {cost:5.0}]->(f),\n" +
                        " (b)-[:TYPE {cost:4.0}]->(f),\n" +
                        " (c)-[:TYPE {cost:1.0}]->(f),\n" +
                        " (d)-[:TYPE {cost:1.0}]->(f),\n" +
                        " (e)-[:TYPE {cost:4.0}]->(f)";

        DB.execute(cypher);
        DB.resolveDependency(Procedures.class).registerProcedure(KShortestPathsProc.class);
    }

    @Test
    public void test() throws Exception {
        final String cypher =
                "MATCH (a:Node{name:'a'}), (f:Node{name:'f'}) " +
                        "CALL algo.kShortestPaths(a, f, 42, 'cost') " +
                        "YIELD resultCount RETURN resultCount";

        // 9 possible paths without loop
        DB.execute(cypher).accept(row -> {
            assertEquals(9, row.getNumber("resultCount").intValue());
            return true;
        });

        /**
         * 10 rels from source graph already in DB + 29 rels from 9 paths
         */
        try (Transaction transaction = DB.beginTx()) {
            assertEquals(39, DB.getAllRelationships().stream().count());
            transaction.success();
        }
    }
}
