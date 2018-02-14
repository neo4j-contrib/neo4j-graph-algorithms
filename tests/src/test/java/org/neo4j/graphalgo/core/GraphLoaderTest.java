package org.neo4j.graphalgo.core;

import org.junit.*;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

/**
 * @author mknblch
 */
public class GraphLoaderTest {

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        DB.execute("FOREACH (x IN range(1, 4098) | CREATE (:Node {index:x}))");
        DB.execute("MATCH (n) WHERE n.index IN [1, 2, 3] DELETE n");
    }

    @Test
    public void testHeavy() {
        new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withAnyRelationshipType()
                .load(HeavyGraphFactory.class);
    }

    @Test
    public void testHuge() {
        new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withAnyRelationshipType()
                .load(HugeGraphFactory.class);
    }
}
