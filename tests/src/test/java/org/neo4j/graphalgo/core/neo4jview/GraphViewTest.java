package org.neo4j.graphalgo.core.neo4jview;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.neo4j.graphalgo.SimpleGraphSetup;
import org.neo4j.graphalgo.SimpleGraphTestCase;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknobloch
 */
@Ignore("weights faulty")
public class GraphViewTest extends SimpleGraphTestCase {

    @BeforeClass
    public static void setupGraph() {
        final SimpleGraphSetup setup = new SimpleGraphSetup();
        graph = new GraphView((GraphDatabaseAPI) setup.getDb(), LABEL, WEIGHT_PROPERTY);
        v0 = 0;
        v1 = 1;
        v2 = 2;
    }
}
