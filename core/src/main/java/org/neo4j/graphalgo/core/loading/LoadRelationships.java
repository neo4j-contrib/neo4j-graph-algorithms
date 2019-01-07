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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;

import java.util.function.Consumer;

public interface LoadRelationships {

    int degreeOut(NodeCursor cursor);

    int degreeIn(NodeCursor cursor);

    int degreeBoth(NodeCursor cursor);

    /**
     * See {@link NodesHelper} for the reason why we have this in addition to {@link #degreeBoth(NodeCursor)}.
     */
    int degreeUndirected(NodeCursor cursor);

    RelationshipSelectionCursor relationshipsOut(NodeCursor cursor);

    RelationshipSelectionCursor relationshipsIn(NodeCursor cursor);

    RelationshipSelectionCursor relationshipsBoth(NodeCursor cursor);

    default RelationshipSelectionCursor relationshipsOf(Direction direction, NodeCursor cursor) {
        switch (direction) {
            case OUTGOING:
                return relationshipsOut(cursor);
            case INCOMING:
                return relationshipsIn(cursor);
            case BOTH:
                return relationshipsBoth(cursor);
            default:
                throw new IllegalArgumentException("direction " + direction);
        }
    }

    static void consumeRelationships(RelationshipSelectionCursor cursor, Consumer<RelationshipSelectionCursor> action) {
        try (RelationshipSelectionCursor rels = cursor) {
            while (rels.next()) {
                action.accept(rels);
            }
        }
    }

    static LoadRelationships of(CursorFactory cursors, int[] relationshipType) {
        if (relationshipType == null || relationshipType.length == 0) {
            return new LoadAllRelationships(cursors);
        }
        return new LoadRelationshipsOfSingleType(cursors, relationshipType);
    }
}


final class LoadAllRelationships implements LoadRelationships {
    private final CursorFactory cursors;

    LoadAllRelationships(final CursorFactory cursors) {
        this.cursors = cursors;
    }

    @Override
    public int degreeOut(final NodeCursor cursor) {
        return Nodes.countOutgoing(cursor, cursors);
    }

    @Override
    public int degreeIn(final NodeCursor cursor) {
        return Nodes.countIncoming(cursor, cursors);
    }

    @Override
    public int degreeBoth(final NodeCursor cursor) {
        return Nodes.countAll(cursor, cursors);
    }

    @Override
    public int degreeUndirected(final NodeCursor cursor) {
        return NodesHelper.countUndirected(cursor, cursors);
    }

    @Override
    public RelationshipSelectionCursor relationshipsOut(final NodeCursor cursor) {
        return RelationshipSelections.outgoingCursor(cursors, cursor, null);
    }

    @Override
    public RelationshipSelectionCursor relationshipsIn(final NodeCursor cursor) {
        return RelationshipSelections.incomingCursor(cursors, cursor, null);
    }

    @Override
    public RelationshipSelectionCursor relationshipsBoth(final NodeCursor cursor) {
        return RelationshipSelections.allCursor(cursors, cursor, null);
    }
}

final class LoadRelationshipsOfSingleType implements LoadRelationships {
    private final CursorFactory cursors;
    private final int type;
    private final int[] types;

    LoadRelationshipsOfSingleType(final CursorFactory cursors, final int[] types) {
        this.cursors = cursors;
        this.type = types[0];
        this.types = types;
    }

    @Override
    public int degreeOut(final NodeCursor cursor) {
        return Nodes.countOutgoing(cursor, cursors, type);
    }

    @Override
    public int degreeIn(final NodeCursor cursor) {
        return Nodes.countIncoming(cursor, cursors, type);
    }

    @Override
    public int degreeBoth(final NodeCursor cursor) {
        return Nodes.countAll(cursor, cursors, type);
    }

    @Override
    public int degreeUndirected(final NodeCursor cursor) {
        return NodesHelper.countUndirected(cursor, cursors, type);
    }

    @Override
    public RelationshipSelectionCursor relationshipsOut(final NodeCursor cursor) {
        return RelationshipSelections.outgoingCursor(cursors, cursor, types);
    }

    @Override
    public RelationshipSelectionCursor relationshipsIn(final NodeCursor cursor) {
        return RelationshipSelections.incomingCursor(cursors, cursor, types);
    }

    @Override
    public RelationshipSelectionCursor relationshipsBoth(final NodeCursor cursor) {
        return RelationshipSelections.allCursor(cursors, cursor, types);
    }
}
