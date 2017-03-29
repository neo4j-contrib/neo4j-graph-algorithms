package org.neo4j.graphalgo.core.heavyweight.mst;

import java.util.ArrayList;

/**
 * @author mknobloch
 */
public class MinimumSpanningTree extends ArrayList<Edge> {

    public boolean containsTarget(int targetNodeId) {
        for (Edge edge : this) {
            if (targetNodeId == edge.targetNode) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        for (Edge edge : this) {
            if (builder.length() != 0) {
                builder.append("\n");
            }
            builder.append(edge);
        }
        return builder.toString();
    }
}
