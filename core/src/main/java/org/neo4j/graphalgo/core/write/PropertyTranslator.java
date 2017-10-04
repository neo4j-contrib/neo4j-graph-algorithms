package org.neo4j.graphalgo.core.write;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public interface PropertyTranslator<T> {

    Value toProperty(int propertyId, T data, long nodeId);

    interface OfDouble<T> extends PropertyTranslator<T> {
        double toDouble(final T data, final long nodeId);

        @Override
        default Value toProperty(
                int propertyId,
                T data,
                long nodeId) {
            return Values.doubleValue(toDouble(data, nodeId));
        }
    }

    interface OfOptionalDouble<T> extends PropertyTranslator<T> {
        double toDouble(final T data, final long nodeId);

        @Override
        default Value toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final double value = toDouble(data, nodeId);
            if (value >= 0D) {
                return Values.doubleValue(value);
            }
            return null;
        }
    }

    interface OfInt<T> extends PropertyTranslator<T> {
        int toInt(final T data, final long nodeId);

        @Override
        default Value toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final int value = toInt(data, nodeId);
            return Values.intValue(value);
        }
    }

    interface OfOptionalInt<T> extends PropertyTranslator<T> {
        int toInt(final T data, final long nodeId);

        @Override
        default Value toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final int value = toInt(data, nodeId);
            if (value >= 0) {
                return Values.intValue(value);
            }
            return null;
        }
    }
}
