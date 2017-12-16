package org.neo4j.graphalgo.impl.spanningTrees;

import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

public class SpanningTree {

    public final int head;
    public final int nodeCount;
    public final int effectiveNodeCount;
    public final int[] parent;

    public SpanningTree(int head, int nodeCount, int effectiveNodeCount, int[] parent) {
        this.head = head;
        this.nodeCount = nodeCount;
        this.effectiveNodeCount = effectiveNodeCount;
        this.parent = parent;
    }

    public void forEach(RelationshipConsumer consumer) {
        for (int i = 0; i < nodeCount; i++) {
            final int parent = this.parent[i];
            if (parent == -1) {
                continue;
            }
            if (!consumer.accept(parent, i, -1L)) {
                return;
            }
        }
    }

    public int head(int node) {
        int p = node;
        while (-1 != parent[p]) {
            p = parent[p];
        }
        return p;
    }

    public static final PropertyTranslator<SpanningTree> TRANSLATOR = new SpanningTreeTranslator();

    public static class SpanningTreeTranslator implements PropertyTranslator.OfInt<SpanningTree> {
        @Override
        public int toInt(final SpanningTree data, final long nodeId) {
            return data.head((int) nodeId);
        }
    }

}