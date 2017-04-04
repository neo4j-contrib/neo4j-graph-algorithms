package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author mknobloch
 */
public class EmptyGraphUnionFindTest extends Neo4JTestCase {

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() {

        graph = new GraphLoader((GraphDatabaseAPI) db)
                .withAnyLabel()
                .withRelationshipType(RELATION)
                .withWeightsFromProperty(WEIGHT_PROPERTY, 0.0)
                .load(HeavyGraphFactory.class);

    }

    @Test
    public void test() throws Exception {

        Map<Integer, List<Integer>> struct = new UnionFind(graph).compute();

        assertTrue(struct.isEmpty());
    }
}
