package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.closeness.HugeMSClosenessCentrality;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.function.BiConsumer;
import java.util.function.DoubleConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
    }

    @Test
    public void testHeavy() throws Exception {
        final Graph graph = new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .asUndirected(true)
                .load(HeavyGraphFactory.class);
        final MSClosenessCentrality algo = new MSClosenessCentrality(graph, 2, Pools.DEFAULT, true );
        final DoubleConsumer mock = mock(DoubleConsumer.class);
        algo.compute()
                .resultStream()
                .peek(System.out::println)
                .forEach(r -> mock.accept(r.centrality));
        verify(mock, times(3)).accept(AdditionalMatchers.eq(0.5, 0.01));
        verify(mock, times(2)).accept(AdditionalMatchers.eq(0.25, 0.01));
    }

    @Test
    public void testHuge() throws Exception {


        final HugeGraph graph = (HugeGraph) new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .asUndirected(true)
                .load(HugeGraphFactory.class);
        final HugeMSClosenessCentrality algo = new HugeMSClosenessCentrality(graph,
                AllocationTracker.EMPTY,
                2,
                Pools.DEFAULT,
                true);
        final DoubleConsumer mock = mock(DoubleConsumer.class);
        algo.compute()
                .resultStream()
                .peek(System.out::println)
                .forEach(r -> mock.accept(r.centrality));

        verify(mock, times(3)).accept(AdditionalMatchers.eq(0.5, 0.01));
        verify(mock, times(2)).accept(AdditionalMatchers.eq(0.25, 0.01));
    }
}
