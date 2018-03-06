package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

/**
 *
 * (a)--(b) (d)--(e)
 *   \  /
 *   (c)
 *
 *
 * @author mknblch
 */
public class ClosenessCentralityDiscoTest {

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();
    private static Graph graph;

    @BeforeClass
    public static void setup() {
        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +

                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (b)-[:TYPE]->(c),\n" +

                        " (d)-[:TYPE]->(e)";

        DB.execute(cypher);

        graph = new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .asUndirected(true)
                .load(HeavyGraphFactory.class);
    }

    @Test
    public void test() throws Exception {

        final MSClosenessCentrality algo = new MSClosenessCentrality(graph, 1, Pools.DEFAULT, true );

        algo.compute()
                .resultStream()
                .forEach(System.out::println);
    }
}
