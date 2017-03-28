package org.neo4j.graphalgo.core.leightweight;

import org.junit.BeforeClass;
import org.neo4j.graphalgo.SimpleGraphTestCase;
import org.neo4j.graphalgo.SimpleGraphSetup;

/**
 * @author mknobloch
 */
public class LightGraphTest extends SimpleGraphTestCase {

    @BeforeClass
    public static void setupGraph() {
        final SimpleGraphSetup setup = new SimpleGraphSetup();
        graph = setup.build(LightGraphFactory.class);
        v0 = setup.getV0();
        v1 = setup.getV1();
        v2 = setup.getV2();
    }
}
