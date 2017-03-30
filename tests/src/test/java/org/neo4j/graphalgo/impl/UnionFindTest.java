package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknobloch
 */
public class UnionFindTest extends Neo4JTestCase {

    private static Graph graph;
    private static int a, b, c;

    @BeforeClass
    public static void setupGraph() {

        final int n1 = newNode();
        final int n2 = newNode();
        final int n3 = newNode();

        newRelation(n1, n2);
        newRelation(n2, n3);
        newRelation(n3, n1);

        graph = new GraphLoader((GraphDatabaseAPI) db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withDefaultWeight(0.0)
                .load(HeavyGraphFactory.class);

        a = graph.toMappedNodeId(n1);
        b = graph.toMappedNodeId(n2);
        c = graph.toMappedNodeId(n3);
    }

    @Ignore("faulty")
    @Test
    public void test() throws Exception {
        final int[] set = new UnionFind(graph).compute();
        for (int i = 0; i < set.length; i++) {
            System.out.println(i + " -> " + set[i]);
        }
    }
}
