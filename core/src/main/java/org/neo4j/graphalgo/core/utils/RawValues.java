package org.neo4j.graphalgo.core.utils;

/**
 * @author mknobloch
 */
public class RawValues {

    /**
     * shifts up the head into the most significant 4 bytes of the long and
     * places the tail in the least significant bytes
     * @param head an arbitrary int value
     * @param tail an arbitrary int value
     * @return the combination of head and
     */
    public static long combineIntInt(int head, int tail) {
        return ((long) head << 32) | tail;
    }

    /**
     * get the head value
     * @param combinedValue a value built of 2 ints
     * @return the most significant 4 bytes as int
     */
    public static int getHead(long combinedValue) {
        return (int) (combinedValue >> 32);
    }

    /**
     * get the tail value
     * @param combinedValue a value built of 2 ints
     * @return the least significant 4 bytes as int
     */
    public static int getTail(long combinedValue) {
        return (int) combinedValue;
    }
}
