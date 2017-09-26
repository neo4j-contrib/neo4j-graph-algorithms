package org.neo4j.graphalgo.core.utils;

public final class ArrayUtil {

    /**
     * Find the index where {@code (ids[idx] <= id) && (ids[idx + 1] > id)}.
     * The result differs from that of {@link java.util.Arrays#binarySearch(long[], long)}
     * in that this method returns a positive index even if the array does not
     * directly contain the searched value.
     * It returns -1 iff the value is smaller than the smallest one in the array.
     */
    public static int binaryLookup(long id, long[] ids) {
        int length = ids.length;

        int low = 0;
        int high = length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = ids[mid];

            if (midVal < id) {
                low = mid + 1;
            } else if (midVal > id) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return low - 1;
    }

    /**
     * Find the index where {@code (ids[idx] <= id) && (ids[idx + 1] > id)}.
     * The result differs from that of {@link java.util.Arrays#binarySearch(long[], long)}
     * in that this method returns a positive index even if the array does not
     * directly contain the searched value.
     * It returns -1 iff the value is smaller than the smallest one in the array.
     */
    public static int binaryLookup(int id, int ids[]) {
        int length = ids.length;

        int low = 0;
        int high = length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = ids[mid];

            if (midVal < id) {
                low = mid + 1;
            } else if (midVal > id) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return low - 1;
    }

    private ArrayUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
