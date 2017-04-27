package org.neo4j.graphalgo.core.heavyweight;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class HeavyCypherGraphFactoryTest {

    private static GraphDatabaseService db;

    private static int id1;
    private static int id2;
    private static int id3;

    @BeforeClass
    public static void setUp() {

        db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        db.execute("CREATE (n1)-[:REL  {prop:1}]->(n2)-[:REL {prop:2}]->(n3) CREATE (n1)-[:REL {prop:3}]->(n3) RETURN id(n1) AS id1, id(n2) AS id2, id(n3) AS id3").accept(row -> {
            id1 = row.getNumber("id1").intValue();
            id2 = row.getNumber("id2").intValue();
            id3 = row.getNumber("id3").intValue();
            return true;
        });
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void testLoadCypher() throws Exception {


        String nodes = "MATCH (n) RETURN id(n) as id";
        String rels = "MATCH (n)-[r:REL]->(m) RETURN id(n) as source, id(m) as target, r.prop as weight";

        final Graph graph = new GraphLoader((GraphDatabaseAPI) db)
                .withRelationshipWeightsFromProperty("prop",0)
                .withNodeStatement(nodes)
                .withRelationshipStatement(rels)
                .load(HeavyCypherGraphFactory.class);

        assertEquals(3, graph.nodeCount());
        assertEquals(2, graph.degree(graph.toMappedNodeId(id1), Direction.OUTGOING));
        assertEquals(1, graph.degree(graph.toMappedNodeId(id2), Direction.OUTGOING));
        assertEquals(0, graph.degree(graph.toMappedNodeId(id3), Direction.OUTGOING));
        AtomicInteger total = new AtomicInteger();
        graph.forEachNode(n -> graph.weightedRelationshipIterator(n, Direction.OUTGOING).forEachRemaining(rel -> total.addAndGet((int)rel.weight)));
        assertEquals(6,total.get());
    }
}
