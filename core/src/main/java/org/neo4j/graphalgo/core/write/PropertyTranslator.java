/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
