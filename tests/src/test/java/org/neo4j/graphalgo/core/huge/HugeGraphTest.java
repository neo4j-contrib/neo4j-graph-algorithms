package org.neo4j.graphalgo.core.huge;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.graphalgo.SimpleGraphSetup;
import org.neo4j.graphalgo.SimpleGraphTestCase;

/**
 * @author mknobloch
 */
public class HugeGraphTest extends SimpleGraphTestCase {

    private static SimpleGraphSetup setup;

    @BeforeClass
    public static void setupGraph() {
        setup = new SimpleGraphSetup();
        graph = setup.build(HugeGraphFactory.class);
        v0 = setup.getV0();
        v1 = setup.getV1();
        v2 = setup.getV2();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (setup != null) setup.shutdown();
        if (db != null) db.shutdown();
    }
}
