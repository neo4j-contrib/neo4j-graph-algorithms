package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.PathCompressingDSS;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *        A   D  F
 *       / \  |
 *      B--C  E
 *
 * @author mknobloch
 */
public class UnionFindTest extends Neo4JTestCase {

    private static Graph graph;
    private static int a, b, c, d, e, f;

    @BeforeClass
    public static void setupGraph() {

        final int n1 = newNode();
        final int n2 = newNode();
        final int n3 = newNode();
        final int n4 = newNode();
        final int n5 = newNode();
        final int n6 = newNode();

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
        f = graph.toMappedNodeId(n6);
    }

    @Test
    public void test() throws Exception {
        final Map<Integer, List<Integer>> struct = new UnionFind(graph).compute();

        assertEquals(3, struct.size());

        assertTrue(struct.get(a).containsAll(Arrays.asList(a, b, c)));
        assertTrue(struct.get(d).containsAll(Arrays.asList(d, e)));
        assertTrue(struct.get(f).containsAll(Arrays.asList(f)));

        System.out.println(struct);
    }
}
