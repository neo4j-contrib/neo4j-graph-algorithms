package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.sources.SingleRunAllRelationIterator;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**         1d
 *        A---D   F
 *       / \   \
 *      B--C    E
 *
 * @author mknobloch
 */
public class UnionFindTest extends Neo4JTestCase {

    private static int a, b, c, d, e, f;

    private static SingleRunAllRelationIterator iterator;
    private static LazyIdMapper idMapper;

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

        newRelation(n1, n4);

        idMapper = new LazyIdMapper();
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
        final DisjointSetStruct struct = new UnionFind(idMapper, iterator).compute();
        assertAllSameSet(struct, a, b, c, d, e);
        assertAllNotSameSet(struct, f, a, b, c, d, e);
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
