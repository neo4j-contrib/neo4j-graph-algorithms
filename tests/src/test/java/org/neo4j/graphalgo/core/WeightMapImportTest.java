package org.neo4j.graphalgo.core;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
@Ignore("Issue #444")
public class WeightMapImportTest {

    private static GraphDatabaseAPI db;
    private static HeavyGraph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (a)-[:TYPE {w:1}]->(b)";


        db = TestDatabaseCreator.createTestDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LouvainProc.class);

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("w", 0.0)
                .load(HeavyGraphFactory.class);

    }

    @AfterClass
    public static void tearDown() {
        if (db != null) db.shutdown();
    }

    private String getName(long nodeId) {
        String[] name = {""};
        db.execute(String.format("MATCH (n) WHERE id(n) = %d RETURN n", nodeId)).accept(row -> {
            name[0] = (String) row.getNode("n").getProperty("name");
            return true;
        });
        return name[0];
    }

    @Test
    public void testWeights() throws Exception {
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                assertEquals(String.format("wrong weight in (%s)->(%s)", getName(sourceNodeId), getName(targetNodeId)),
                        1.0,
                        graph.weightOf(sourceNodeId, targetNodeId), 0.0);
                return true;
            });
            return true;
        });
    }
}
