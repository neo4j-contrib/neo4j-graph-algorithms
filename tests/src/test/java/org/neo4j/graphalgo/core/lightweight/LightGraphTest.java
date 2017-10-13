package org.neo4j.graphalgo.core.lightweight;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.graphalgo.SimpleGraphSetup;
import org.neo4j.graphalgo.SimpleGraphTestCase;

/**
 * @author mknobloch
 */
public class LightGraphTest extends SimpleGraphTestCase {

    private static SimpleGraphSetup setup;

    @BeforeClass
    public static void setupGraph() {
        setup = new SimpleGraphSetup();
        graph = setup.build(LightGraphFactory.class);
        v0 = setup.getV0();
        v1 = setup.getV1();
        v2 = setup.getV2();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
        if (setup != null) setup.shutdown();
    }
}
