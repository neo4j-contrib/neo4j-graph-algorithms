package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphdb.Direction;

import java.util.function.ToLongFunction;

/**
 * TODO: find suitable name or move
 *
 * @author mknblch
 */
public class RawValues {

    public static final IdCombiner OUTGOING = RawValues::combineIntInt;

    public static final IdCombiner INCOMING = (s, e) -> RawValues.combineIntInt(e, s);

    /**
     * shifts head into the most significant 4 bytes of the long
     * and places the tail in the least significant bytes
     * @param head an arbitrary int value
     * @param tail an arbitrary int value
     * @return combination of head and tail
     */
    public static long combineIntInt(int head, int tail) {
        return ((long) head << 32) | tail & 0xFFFFFFFFL ;
    }

    public static IdCombiner combiner(Direction direction) {
        return direction == Direction.OUTGOING ? OUTGOING : INCOMING;
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

    /**
     * convert property value to double
     *
     * @param value the value object
     * @param propertyDefaultValue default value if property cant be converted
     * @return double representation of value
     */
    public static double extractValue(Object value, double propertyDefaultValue) {
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.doubleValue();
        }
        if (value instanceof String) {
            String s = (String) value;
            if (!s.isEmpty()) {
                return Double.parseDouble(s);
            }
        }
        if (value instanceof Boolean) {
            if ((Boolean) value) {
                return 1d;
            }
        }
        // TODO: arrays

        return propertyDefaultValue;
    }
}
