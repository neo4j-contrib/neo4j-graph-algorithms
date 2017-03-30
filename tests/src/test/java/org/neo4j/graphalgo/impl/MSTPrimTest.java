package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.MSTPrim;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphalgo.core.utils.RawValues.*;

/**
 * Tests if MSTPrim returns a correct MST
 *
 *         a                a
 *     1 /   \ 2          /  \
 *      /     \          /    \
 *     b --3-- c        b      c
 *     |       |   =>   |      |
 *     4       5        |      |
 *     |       |        |      |
 *     d --6-- e        d      e
 *
 *
 * @author mknobloch
 */
public class MSTPrimTest extends Neo4JTestCase {

    private static int a, b, c, d, e;
    private static Graph graph;

    @BeforeClass
    public static void setupGraph() {
        a = newNode();
        b = newNode();
        c = newNode();
        d = newNode();
        e = newNode();

        newRelation(a, b, 1);
        newRelation(a, c, 2);
        newRelation(b, c, 3);
        newRelation(b, d, 4);
        newRelation(c, e, 5);
        newRelation(d, e, 6);

        graph = new GraphLoader((GraphDatabaseAPI) db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withWeightsFromProperty(WEIGHT_PROPERTY, Double.MAX_VALUE)
                .load(HeavyGraphFactory.class);

        a = graph.toMappedNodeId(a);
        b = graph.toMappedNodeId(b);
        c = graph.toMappedNodeId(c);
        d = graph.toMappedNodeId(d);
        e = graph.toMappedNodeId(e);
    }

    @Test
    public void testPrim() throws Exception {

        final LongArrayList mst = new MSTPrim(graph).compute(a);

        for (LongCursor cursor : mst) {
            System.out.println(var(getHead(cursor.value)) + " -> " + var(getTail(cursor.value)));
        }

        assertTrue(mst.contains(combineIntInt(a, b)));
        assertTrue(mst.contains(combineIntInt(a, c)));
        assertTrue(mst.contains(combineIntInt(b, d)));
        assertTrue(mst.contains(combineIntInt(c, e)));
    }

    @Ignore("current impl. only takes outgoing relations into account")
    @Test
    public void test2() throws Exception {
        final LongArrayList mst = new MSTPrim(graph).compute(b);
        assertEquals(4, mst.size());

        for (LongCursor cursor : mst) {
            System.out.println(var(getHead(cursor.value)) + " -> " + var(getTail(cursor.value)));
        }
    }

    public String var(int id) {
        if (id == a) {
            return "a";
        }
        if (id == b) {
            return "b";
        }
        if (id == c) {
            return "c";
        }
        if (id == d) {
            return "d";
        }
        if (id == e) {
            return "e";
        }
        return "null";
    }
}
