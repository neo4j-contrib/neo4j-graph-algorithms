package org.neo4j.graphalgo.core;

import org.junit.*;
import org.junit.rules.ErrorCollector;
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

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    @BeforeClass
    public static void setupGraph() {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 4098; i++) {
            builder.append("CREATE (n").append(i).append(":Node {index:").append(i).append("})\n");
        }
        DB.execute(builder.toString());
        DB.execute("MATCH (n) WHERE n.index IN [1, 2, 3] DELETE n");
    }

    @Ignore("heavy is not affected")
    @Test
    public void testHeavy() throws Exception {
        new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withAnyRelationshipType()
                .load(HeavyGraphFactory.class);
    }

    @Test
    public void testHuge() throws Exception {
        new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withAnyRelationshipType()
                .load(HugeGraphFactory.class);
    }
}
