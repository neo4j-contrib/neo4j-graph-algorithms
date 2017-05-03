package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.sources.BothRelationshipAdapter;
import org.neo4j.graphalgo.core.sources.BufferedWeightMap;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.utils.container.RelationshipContainer;
import org.neo4j.graphalgo.core.utils.container.SubGraph;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    private static LazyIdMapper idMapper;
    private static BufferedWeightMap weightMap;
    private static BothRelationshipAdapter bothRelationshipAdapter;

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

        idMapper = new LazyIdMapper(5);

        weightMap = BufferedWeightMap.importer((GraphDatabaseAPI) db)
                .withIdMapping(idMapper)
                .withAnyDirection(true)
                .withLabel(LABEL)
                .withRelationshipType(RELATION)
                .withWeightsFromProperty(WEIGHT_PROPERTY, 0.0)
                .build();

        RelationshipContainer relationshipContainer = RelationshipContainer.importer((GraphDatabaseAPI) db)
                .withIdMapping(idMapper)
                .withDirection(Direction.BOTH)
                .withLabel(LABEL)
                .withRelationshipType(RELATION)
                .withWeightsFromProperty(WEIGHT_PROPERTY, 0.0)
                .build();

        bothRelationshipAdapter = new BothRelationshipAdapter(relationshipContainer);

        a = idMapper.toMappedNodeId(a);
        b = idMapper.toMappedNodeId(b);
        c = idMapper.toMappedNodeId(c);
        d = idMapper.toMappedNodeId(d);
        e = idMapper.toMappedNodeId(e);
    }

    @Test
    public void testMstFromA() throws Exception {
        verifyMst(new MSTPrim(idMapper, bothRelationshipAdapter, weightMap).compute(a).getMinimumSpanningTree());
    }

    @Test
    public void testMstFromB() throws Exception {
        verifyMst(new MSTPrim(idMapper, bothRelationshipAdapter, weightMap).compute(b).getMinimumSpanningTree());
    }

    @Test
    public void testMstFromC() throws Exception {
        verifyMst(new MSTPrim(idMapper, bothRelationshipAdapter, weightMap).compute(c).getMinimumSpanningTree());
    }

    @Test
    public void testMstFromD() throws Exception {
        verifyMst(new MSTPrim(idMapper, bothRelationshipAdapter, weightMap).compute(d).getMinimumSpanningTree());
    }

    @Test
    public void testMstFromE() throws Exception {
        verifyMst(new MSTPrim(idMapper, bothRelationshipAdapter, weightMap).compute(d).getMinimumSpanningTree());
    }

    private void verifyMst(MSTPrim.MinimumSpanningTree mst) {
        final AssertingConsumer consumer = new AssertingConsumer();
        mst.forEachDFS(a, consumer);
        consumer.assertContains(a, b);
        consumer.assertContains(a, c);
        consumer.assertContains(b, d);
        consumer.assertContains(c, e);
    }

    private static class AssertingConsumer implements RelationshipConsumer {

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

        @Override
        public boolean accept(int sourceNodeId, int targetNodeId, @Deprecated long relationId) {
            pairs.add(new Pair(
                    Math.min(sourceNodeId, targetNodeId),
                    Math.max(sourceNodeId, targetNodeId)));
            return true;
        }

        public void assertSize(int expected) {
            assertEquals("size does not match", expected, pairs.size());
        }

        public void assertContains(int a, int b) {
            assertTrue("{" + a + "," + b + "} not found",
                    pairs.contains(new Pair(Math.min(a, b), Math.max(a, b))));
        }
    }
}
