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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.internal.kernel.api.Read;

final class ImportRange {
    final long relationshipStartId;
    final long relationshipEndId;
    private final int[] relType;

    static final ImportRange ALL = new ImportRange(0L, Long.MAX_VALUE, null);

    ImportRange(
            long relationshipStartId,
            long relationshipEndId,
            int[] relType) {
        this.relationshipStartId = relationshipStartId;
        this.relationshipEndId = relationshipEndId;
        this.relType = relType;
    }

    boolean isAllRelationships() {
        return (relationshipEndId - relationshipStartId) == Long.MAX_VALUE;
    }

    int relationshipType() {
        if (relType != null && relType.length > 0) {
            return relType[0];
        }
        return Read.ANY_RELATIONSHIP_TYPE;
    }
}
