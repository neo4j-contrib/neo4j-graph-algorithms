package org.neo4j.graphalgo.core.heavyweight;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.neo4j.graphalgo.SimpleGraphTestCase;
import org.neo4j.graphalgo.SimpleGraphSetup;

/**
 * @author mknobloch
 */
//@Ignore("weights not implemented yet")
public class HeavyGraphTest extends SimpleGraphTestCase {

    static SimpleGraphSetup setup = new SimpleGraphSetup();
    @BeforeClass
    public static void setupGraph() {

        graph = setup.build(HeavyGraphFactory.class);
        v0 = setup.getV0();
        v1 = setup.getV1();
        v2 = setup.getV2();
    }
    @AfterClass
    public static void tearDown() throws Exception {
        if (setup != null) setup.getDb().shutdown();
        if (db!=null) db.shutdown();
    }

}
