package org.neo4j.graphalgo.impl.mst;

import org.junit.Test;
import org.neo4j.graphalgo.core.heavyweight.mst.Edge;
import org.neo4j.graphalgo.core.heavyweight.mst.EdgeQueue;

import static org.junit.Assert.assertEquals;

/**
 * @author mknobloch
 */
public class EdgeQueueTest {

    @Test
    public void test() throws Exception {

        final EdgeQueue queue = new EdgeQueue(3);

        queue.add(new Edge(1, 4, 2));
        queue.add(new Edge(1, 3, 1));
        queue.add(new Edge(1, 2, 0));

        assertEquals(2, queue.poll().targetNode);
        assertEquals(3, queue.poll().targetNode);
        assertEquals(4, queue.poll().targetNode);
    }
}
