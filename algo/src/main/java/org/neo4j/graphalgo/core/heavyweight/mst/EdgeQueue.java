package org.neo4j.graphalgo.core.heavyweight.mst;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author mknobloch
 */
public class EdgeQueue extends PriorityQueue<Edge> {

    private static final Comparator<Edge> COMPARATOR = new Comparator<Edge>() {
        @Override
        public int compare(Edge o1, Edge o2) {
            return Double.compare(o1.weight, o2.weight);
        }
    };

    public EdgeQueue(int initialCapacity) {
        super(initialCapacity, COMPARATOR);
    }

    public void add(int sourceNodeId, int targetNodeId, double weight) {
        super.add(new Edge(sourceNodeId, targetNodeId, weight));
    }
}
