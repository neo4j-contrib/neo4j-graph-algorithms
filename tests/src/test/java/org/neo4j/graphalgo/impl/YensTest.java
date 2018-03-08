package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.List;
import java.util.function.DoubleConsumer;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
public class YensTest {

    public static final double DELTA = 0.001;

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

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

        db.execute(cypher);

        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withDirection(Direction.BOTH)
                .load(HeavyGraphFactory.class);
    }

    @Test
    public void test() throws Exception {

        final YensKShortestPaths yens = new YensKShortestPaths(graph).compute(
                getNode("a").getId(),
                getNode("f").getId(),
                Direction.BOTH,
                7);

        final List<WeightedPath> paths = yens.getPaths();

        final DoubleConsumer mock = mock(DoubleConsumer.class);

        for (int i = 0; i < paths.size(); i++) {
            final WeightedPath path = paths.get(i);
            mock.accept(path.getCost(graph));
            System.out.println(path + " = "  + path.getCost(graph));
        }

        verify(mock, times(2)).accept(eq(3.0, DELTA));
        verify(mock, times(1)).accept(eq(4.0, DELTA));
        verify(mock, times(4)).accept(eq(5.0, DELTA));
    }

    private static Node getNode(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return node[0];
    }
}
