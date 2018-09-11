package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.LongHashSet;

import java.util.Arrays;

public class Intersections {
    public static long intersection(LongHashSet targets1, LongHashSet targets2) {
        LongHashSet intersectionSet = new LongHashSet(targets1);
        intersectionSet.retainAll(targets2);
        return intersectionSet.size();
    }

    public static long intersection2(long[] targets1, long[] targets2) {
        LongHashSet intersectionSet = LongHashSet.from(targets1);
        intersectionSet.retainAll(LongHashSet.from(targets2));
        return intersectionSet.size();
    }

    // assume both are sorted
    public static long intersection3(long[] targets1, long[] targets2) {
        int len2;
        if ((len2 = targets2.length) == 0) return 0;
        int off2 = 0;
        long intersection = 0;
        for (long value1 : targets1) {
            if (value1 > targets2[off2]) {
                while (++off2 != len2 && value1 > targets2[off2]);
                if (off2 == len2) return intersection;
            }
            if (value1 == targets2[off2]) {
                intersection++;
                off2++;
                if (off2 == len2) return intersection;
            }
        }
        return intersection;
    }

    // idea, compute differences, when 0 then equal?
    // assume both are sorted
    public static long intersection4(long[] targets1, long[] targets2) {
        if (targets2.length == 0) return 0;
        int off2 = 0;
        long intersection = 0;
        for (int off1 = 0; off1 < targets1.length; off1++) {
            if (off2 == targets2.length) return intersection;
            long value1 = targets1[off1];

            if (value1 > targets2[off2]) {
                for (;off2 < targets2.length;off2++) {
                    if (value1 <= targets2[off2]) break;
                }
                // while (++off2 != targets2.length && value1 > targets2[off2]);
                if (off2 == targets2.length) return intersection;
            }
            if (value1 == targets2[off2]) {
                intersection++;
                off2++;
            }
        }
        return intersection;
    }

    private static int[] EMPTY = new int[0];

    public static int[] getIntersection(int[] values1, int[] values2) {
        if (values1==null || values2 == null) return EMPTY;
        return getIntersection(values1, values1.length, values2,values2.length);
    }
    public static int[] getIntersection(int[] values1, int len1, int[] values2, int len2) {
        if (values1==null || values2 == null) return EMPTY;
        if (len1 == 0 || len2 == 0) return EMPTY;
        int off2 = 0;
        int[] intersection = new int[Math.min(len1,len2)];
        int resIdx = 0;
        for (int i = 0; i < len1; i++) {
            int value1 = values1[i];
            if (value1 > values2[off2]) {
                while (++off2 != len2 && value1 > values2[off2]);
                if (off2 == len2) break;
            }
            if (value1 == values2[off2]) {
                intersection[resIdx++]=value1;
                off2++;
                if (off2 == len2) break;
            }
        }
        return resIdx < intersection.length ? Arrays.copyOf(intersection, resIdx) : intersection;
    }
}
