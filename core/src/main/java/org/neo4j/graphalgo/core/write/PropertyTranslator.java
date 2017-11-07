package org.neo4j.graphalgo.core.write;

import org.neo4j.kernel.api.properties.DefinedProperty;

public interface PropertyTranslator<T> {

    DefinedProperty toProperty(int propertyId, T data, long nodeId);

    interface OfDouble<T> extends PropertyTranslator<T> {
        double toDouble(final T data, final long nodeId);

        @Override
        default DefinedProperty toProperty(
                int propertyId,
                T data,
                long nodeId) {
            return DefinedProperty.doubleProperty(
                    propertyId,
                    toDouble(data, nodeId)
            );
        }
    }

    interface OfOptionalDouble<T> extends PropertyTranslator<T> {
        double toDouble(final T data, final long nodeId);

        @Override
        default DefinedProperty toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final double value = toDouble(data, nodeId);
            if (value >= 0D) {
                return DefinedProperty.doubleProperty(
                        propertyId,
                        value
                );
            }
            return null;
        }
    }

    interface OfInt<T> extends PropertyTranslator<T> {
        int toInt(final T data, final long nodeId);

        @Override
        default DefinedProperty toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final int value = toInt(data, nodeId);
            return DefinedProperty.intProperty(
                    propertyId,
                    value
            );
        }
    }

    interface OfOptionalInt<T> extends PropertyTranslator<T> {
        int toInt(final T data, final long nodeId);

        @Override
        default DefinedProperty toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final int value = toInt(data, nodeId);
            if (value >= 0) {
                return DefinedProperty.intProperty(
                        propertyId,
                        value
                );
            }
            return null;
        }
    }

    interface OfLong<T> extends PropertyTranslator<T> {
        long toLong(final T data, final long nodeId);

        @Override
        default DefinedProperty toProperty(
                int propertyId,
                T data,
                long nodeId) {
            final long value = toLong(data, nodeId);
            return DefinedProperty.longProperty(
                    propertyId,
                    value
            );
        }
    }
}
