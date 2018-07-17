package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.KShortestPathsProc;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Graph:
 * <p>
 * (0)
 * /  |  \
 * (4)--(5)--(1)
 * \  /  \ /
 * (3)---(2)
 *
 * @author mknblch
 */
public class YensKShortestPathsRelationshipCostsTest {

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE {cost:3.0}]->(b),\n" +
                        " (b)-[:TYPE {cost:2.0}]->(c)";

        DB.execute(cypher);
        DB.resolveDependency(Procedures.class).registerProcedure(KShortestPathsProc.class);
    }

    @Test
    public void test() {
        final String cypher =
                "MATCH (c:Node{name:'c'}), (a:Node{name:'a'}) " +
                "CALL algo.kShortestPaths(c, a, 1, 'cost') " +
                "YIELD resultCount RETURN resultCount";

        DB.execute(cypher).accept(row -> {
            assertEquals(1, row.getNumber("resultCount").intValue());
            return true;
        });

        final String shortestPathsQuery = "MATCH p=(:Node {name: $one})-[r:PATH_0*]->(:Node {name: $two})\n" +
                "UNWIND relationships(p) AS pair\n" +
                "return sum(pair.weight) AS distance";

        DB.execute(shortestPathsQuery, MapUtil.map("one", "c", "two", "a")).accept(row -> {
            assertEquals(5.0, row.getNumber("distance").doubleValue(), 0.01);
            return true;
        });


    }
}
