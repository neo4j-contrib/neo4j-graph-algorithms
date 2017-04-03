package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IntBinaryConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
    public void testMstPrim() throws Exception {
        final MSTPrim prim = new MSTPrim(graph);
        verifyMst(prim.compute(a));
        verifyMst(prim.compute(b));
        verifyMst(prim.compute(c));
        verifyMst(prim.compute(d));
        verifyMst(prim.compute(e));
    }

    private void verifyMst(MSTPrim.MST mst) {
        final AssertingTransitionConsumer consumer = new AssertingTransitionConsumer();
        mst.forEach(consumer);
        consumer.assertContains(a, b);
        consumer.assertContains(a, c);
        consumer.assertContains(b, d);
        consumer.assertContains(c, e);
    }

    private static class AssertingTransitionConsumer implements IntBinaryConsumer {

        private static class Pair {
            final int a;
            final int b;
            private Pair(int a, int b) {
                this.a = a;
                this.b = b;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Pair pair = (Pair) o;

                if (a != pair.a) return false;
                return b == pair.b;

            }

            @Override
            public int hashCode() {
                int result = a;
                result = 31 * result + b;
                return result;
            }
        }

        private ArrayList<Pair> pairs = new ArrayList<>();

        public void assertSize(int expected) {
            assertEquals("size does not match", expected, pairs.size());
        }

        @Override
        public void accept(int p, int q) {
            pairs.add(new Pair(Math.min(p, q), Math.max(p, q)));
        }

        public void assertContains(int p, int q) {
            assertTrue("{" + p + "," + q + "} not found", pairs.contains(new Pair(Math.min(p, q), Math.max(p, q))));
        }
    }
}
