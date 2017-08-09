package org.neo4j.graphalgo.algo;

import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.LabelPropagationProc;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.LabelPropagation;
import org.neo4j.graphalgo.impl.SCCIterativeTarjan;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 *    (b)
 *    /  \
 *  (a)--(c)-->(d)-->(e)<==>(f)
 *
 * @author mknblch
 */
public class LabelPropagationTest {


    private static GraphDatabaseAPI api;

    private static HeavyGraph graph;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (a:User {name:'a'})\n" +
                        "CREATE (b:User {name:'b'})\n" +
                        "CREATE (c:User {name:'c'})\n" +
                        "CREATE (d:User {name:'d'})\n" +
                        "CREATE (e:User {name:'e'})\n" +
                        "CREATE (f:User {name:'f'})\n" +
                        "CREATE" +

                        " (a)-[:FOLLOW]->(b),\n" +
                        " (b)-[:FOLLOW]->(a),\n" +

                        " (a)-[:FOLLOW]->(c),\n" +
                        " (c)-[:FOLLOW]->(a),\n" +

                        " (b)-[:FOLLOW]->(c),\n" +
                        " (c)-[:FOLLOW]->(b),\n" +

                        " (c)-[:FOLLOW]->(d),\n" +

                        " (d)-[:FOLLOW]->(e),\n" +

                        " (e)-[:FOLLOW]->(f),\n" +
                        " (f)-[:FOLLOW]->(e)";

        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LabelPropagationProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }

        graph = (HeavyGraph) new GraphLoader(api)
                .withLabel("User")
                .withRelationshipType("Follow")
                .withoutRelationshipWeights()
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    public static int getMappedNodeId(String name) {
        final Node[] node = new Node[1];
        api.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return graph.toMappedNodeId(node[0].getId());
    }

    @Test
    public void testDirect() throws Exception {
        final IntDoubleMap map = new LabelPropagation(graph, Pools.DEFAULT)
                .compute(Direction.OUTGOING, 10, 100_000);
        assertEquals(6, map.size());
    }

    @Test
    public void testCypher() throws Exception {
        String c1 = "CALL algo.labelPropagation('User', 'FOLLOW','OUTGOING', {" +
                "iterations:200,partitionProperty:'partition', write:true}) \n" +
                "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, write, partitionProperty";

        api.execute(c1).accept(row -> {
            assertEquals(6, row.getNumber("nodes").intValue());
            return false;
        });

        String c2 = "MATCH (n) WHERE exists(n.partition) RETURN id(n) as id, n.partition as p";
        api.execute(c2).accept(row -> {
            final long id = row.getNumber("id").longValue();
            final long partition = row.getNumber("p").longValue();
            System.out.println(id + " -> " + partition);
            return true;
        });
    }
}
