package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.sorting.IndirectComparator;

public final class HppcMapComparator implements IndirectComparator {

        private final int[] values;
        private final int[] keys;
        private final int max;
        private boolean hasEmptyKey;

        HppcMapComparator(IntIntHashMap map) {
            values = map.values;
            keys = map.keys;
            max = keys.length - 1;
            int lastValue = values[max];
            hasEmptyKey = map.getOrDefault(0, ~lastValue) == lastValue;
        }

        @Override
        public int compare(final int indexA, final int indexB) {
            assert validIndex(indexA);
            assert validIndex(indexB);

            // assigned slot for A
            if ((keys[indexA] != 0) || (indexA == max && hasEmptyKey)) {

                // assigned slot for B
                if ((keys[indexB] != 0) || (indexB == max && hasEmptyKey)) {

                    // reverse order because descending
                    return Integer.compare(values[indexB], values[indexA]);
                }

                // empty slot for B, put it at the end, A is "smaller"
                return -1;
            }

            // assigned slot for B
            if ((keys[indexB] != 0) || (indexB == max && hasEmptyKey)) {

                // empty slot for A, put it at the end, B is "smaller"
                return 1;
            }

            // empty slot for A and B, both are equal
            return 0;
        }

        private boolean validIndex(int index) {
            assert index >= 0 : "The index " + index + " must point at an existing key.";
            assert index <= max;
            return true;
        }
    }