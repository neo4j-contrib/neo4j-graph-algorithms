package org.neo4j.graphalgo.impl.mst;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.mst.MinimumSpanningTree;
import org.neo4j.graphalgo.impl.MSTPrim;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 *         a
 *     1 /   \ 2
 *      /     \
 *     b --3-- c
 *     |       |
 *     4       5
 *     |       |
 *     d --6-- e
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

    }

    @Test
    public void testPrim() throws Exception {

        final MinimumSpanningTree tree = new MSTPrim(graph).compute(a);

        System.out.println(tree);
    }
}
