package org.neo4j.graphalgo.impl.closeness;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.neo4j.graphalgo.impl.closeness.MSBFSCCAlgorithm.centrality;

public class MSBFSCCAlgorithmTest {
    @Test
    public void testCentralityFormula() {
        /*
            C(u) = \frac{n - 1}{\sum_{v=1}^{n-1} d(v, u)}

            C_{WF}(u) = \frac{n-1}{N-1} \frac{n - 1}{\sum_{v=1}^{n-1} d(v, u)}

            where `d(v, u)` is the shortest-path distance between `v` and `u`
                  `n` is the number of nodes that can reach `u`
                  `N` is the number of nodes in the graph
         */

        assertEquals(1.0, centrality(5, 5, 10, false), 0.01);
        assertEquals(0.5, centrality(10, 5, 10, false), 0.01);
        assertEquals(0, centrality(0, 0, 10, false), 0.01);

        assertEquals(0.44444, centrality(5, 5, 10, true), 0.01);
        assertEquals(1.0, centrality(5, 5, 5, true), 0.01);
    }
}
