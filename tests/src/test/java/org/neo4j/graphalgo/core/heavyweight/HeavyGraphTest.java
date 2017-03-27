package org.neo4j.graphalgo.core.heavyweight;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.neo4j.graphalgo.SimpleGraphTestCase;
import org.neo4j.graphalgo.SimpleGraphSetup;

/**
 * @author mknobloch
 */
//@Ignore("weights not implemented yet")
public class HeavyGraphTest extends SimpleGraphTestCase {

    @BeforeClass
    public static void setupGraph() {
        final SimpleGraphSetup setup = new SimpleGraphSetup();
        graph = setup.build(HeavyGraphFactory.class);
        v0 = setup.getV0();
        v1 = setup.getV1();
        v2 = setup.getV2();
    }
}
