package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.core.sources.BufferedWeightMap;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.sources.SingleRunAllRelationIterator;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.*;

/**         1d
 *        A---D   F
 *       / \   \
 *      B--C    E
 *
 * @author mknobloch
 */
public class ThresholdUnionFindTest extends Neo4JTestCase {

    private static int a, b, c, d, e, f;

    private static SingleRunAllRelationIterator iterator;
    private static LazyIdMapper idMapper;
    private static BufferedWeightMap weightMap;

    @BeforeClass
    public static void setupGraph() {

        final int n1 = newNode();
        final int n2 = newNode();
        final int n3 = newNode();
        final int n4 = newNode();
        final int n5 = newNode();
        final int n6 = newNode();

        newRelation(n1, n2, 3d);
        newRelation(n2, n3, 3d);
        newRelation(n3, n1, 3d);

        newRelation(n4, n5, 3d);

        newRelation(n1, n4, 1d);

        idMapper = new LazyIdMapper();

        weightMap = BufferedWeightMap.importer((GraphDatabaseAPI) db)
                .withIdMapping(idMapper)
                .withAnyLabel()
                .withRelationshipType(RELATION)
                .withWeightsFromProperty(WEIGHT_PROPERTY, 0.0)
                .build();

        iterator = new SingleRunAllRelationIterator((GraphDatabaseAPI) db, idMapper);

        a = idMapper.toMappedNodeId(n1);
        b = idMapper.toMappedNodeId(n2);
        c = idMapper.toMappedNodeId(n3);
        d = idMapper.toMappedNodeId(n4);
        e = idMapper.toMappedNodeId(n5);
        f = idMapper.toMappedNodeId(n6);
    }

    /**
     * should split the graph into 2 distinct sets
     *
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
        final DisjointSetStruct struct = new ThresholdUnionFind(idMapper, iterator, weightMap, 2d).compute();
        assertAllSameSet(struct, a, b, c); // abc belong to a set
        assertAllSameSet(struct, d, e); // as well as d and e
        assertNotEquals(struct.find(a), struct.find(d)); // both sets don't belong to each other
        assertAllNotSameSet(struct, f, a, b, c, d, e); // f is noch connected to any of the sets
    }

    public static void assertAllSameSet(DisjointSetStruct dss, int needle, int... elements) {
        final int setId = dss.find(needle);
        for (int i = 0; i < elements.length; i++) {
            assertEquals("element " + elements[i] + " does not belong to set " + setId,
                    setId,
                    dss.find(elements[i]));
        }
    }

    public static void assertAllNotSameSet(DisjointSetStruct dss, int needle, int... elements) {
        final int setId = dss.find(needle);
        for (int i = 0; i < elements.length; i++) {
            assertNotEquals("element " + elements[i] + " belongs to set " + setId + " but should not",
                    setId,
                    dss.find(elements[i]));
        }
    }
}
