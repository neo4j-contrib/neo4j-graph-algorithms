package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightedRelationConsumer;
import org.neo4j.graphalgo.api.WeightedRelationCursor;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.MinimumSpanningTree;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests if MSTPrim returns a valid MST for each node
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
                .withRelationshipType(RELATION)
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

    private void verifyMst(MinimumSpanningTree mst) {
        final AssertingConsumer consumer = new AssertingConsumer();
        mst.forEach(consumer);
        System.out.println(mst);
        consumer.assertContains(a, b, 1);
        consumer.assertContains(a, c, 2);
        consumer.assertContains(b, d, 4);
        consumer.assertContains(c, e, 5);
    }

    private static class AssertingConsumer implements Consumer<WeightedRelationCursor> {

        private static class Pair {

            final int a;
            final int b;
            final double c;

            private Pair(int a, int b, double c) {
                this.a = a;
                this.b = b;
                this.c = c;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Pair pair = (Pair) o;
                if (a != pair.a) return false;
                if (b != pair.b) return false;
                return Double.compare(pair.c, c) == 0;
            }

            @Override
            public int hashCode() {
                int result;
                long temp;
                result = a;
                result = 31 * result + b;
                temp = Double.doubleToLongBits(c);
                result = 31 * result + (int) (temp ^ (temp >>> 32));
                return result;
            }
        }
        private ArrayList<Pair> pairs = new ArrayList<>();

        @Override
        public void accept(WeightedRelationCursor cursor) {
            pairs.add(new Pair(
                    Math.min(cursor.sourceNodeId, cursor.targetNodeId),
                    Math.max(cursor.sourceNodeId, cursor.targetNodeId),
                    cursor.weight));

        }

        public void assertSize(int expected) {
            assertEquals("size does not match", expected, pairs.size());
        }

        public void assertContains(int a, int b, int c) {
            assertTrue("{" + a + "," + b + "," + c + "} not found", pairs.contains(new Pair(
                    Math.min(a, b),
                    Math.max(a, b),
                    c)));
        }
    }
}
