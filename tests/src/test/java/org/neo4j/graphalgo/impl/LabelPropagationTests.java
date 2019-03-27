package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.Labels;

public final class LabelPropagationTests {

    public static LongObjectMap<LongArrayList> groupByPartition(Labels labels) {
        if (labels == null) {
            return null;
        }
        LongObjectMap<LongArrayList> cluster = new LongObjectHashMap<>();
        for (long node = 0L, l = labels.size(); node < l; node++) {
            long key = labels.labelFor(node);
            LongArrayList ids = cluster.get(key);
            if (ids == null) {
                ids = new LongArrayList();
                cluster.put(key, ids);
            }
            ids.add(node);
        }

        return cluster;
    }

    public static IntObjectMap<IntArrayList> groupByPartitionInt(Labels labels) {
        if (labels == null) {
            return null;
        }
        IntObjectMap<IntArrayList> cluster = new IntObjectHashMap<>();
        for (int node = 0, l = Math.toIntExact(labels.size()); node < l; node++) {
            int key = Math.toIntExact(labels.labelFor(node));
            IntArrayList ids = cluster.get(key);
            if (ids == null) {
                ids = new IntArrayList();
                cluster.put(key, ids);
            }
            ids.add(node);
        }

        return cluster;
    }

    private LabelPropagationTests() {
        throw new UnsupportedOperationException("No instances");
    }
}
