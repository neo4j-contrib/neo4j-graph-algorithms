package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.DisjointSetStruct;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author mknobloch
 */
public class UnionFindTest extends Neo4JTestCase {

    private static Graph graph;
    private static int a, b, c, d, e;

    @BeforeClass
    public static void setupGraph() {

        final int n1 = newNode();
        final int n2 = newNode();
        final int n3 = newNode();
        final int n4 = newNode();
        final int n5 = newNode();

        newRelation(n1, n2);
        newRelation(n2, n3);
        newRelation(n3, n1);

        newRelation(n4, n5);

        graph = new GraphLoader((GraphDatabaseAPI) db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withDefaultWeight(0.0)
                .load(HeavyGraphFactory.class);

        a = graph.toMappedNodeId(n1);
        b = graph.toMappedNodeId(n2);
        c = graph.toMappedNodeId(n3);
        d = graph.toMappedNodeId(n4);
        e = graph.toMappedNodeId(n5);
    }

    @Test
    public void test() throws Exception {
        final DisjointSetStruct struct = new UnionFind(graph).compute();

        assertTrue(struct.connected(a, b));
        assertTrue(struct.connected(a, c));
        assertTrue(struct.connected(b, c));

        assertTrue(struct.connected(d, e));

        assertFalse(struct.connected(a, d));
        assertFalse(struct.connected(a, e));
        assertFalse(struct.connected(b, d));
        assertFalse(struct.connected(b, e));
        assertFalse(struct.connected(c, d));
        assertFalse(struct.connected(c, e));
    }
}
