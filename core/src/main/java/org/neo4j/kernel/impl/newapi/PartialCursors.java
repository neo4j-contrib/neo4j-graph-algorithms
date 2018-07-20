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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;

import java.util.Arrays;

public final class PartialCursors {

    public static RelationshipScanCursor allocateNewCursor(CursorFactory cursors) {
        if (cursors instanceof DefaultCursors) {
            return new PartialRelationshipScanCursor((DefaultCursors) cursors);
        }
        throw new IllegalArgumentException("Unexpected cursor factory: " + cursors);
    }

    public static void partialScan(
            org.neo4j.internal.kernel.api.Read read,
            int type,
            long from,
            long to,
            RelationshipScanCursor cursor) {
        if (!(read instanceof Read)) {
            throw new IllegalArgumentException("Unexpected read: " + read);
        }
        if (!(cursor instanceof PartialRelationshipScanCursor)) {
            throw new IllegalArgumentException("Unexpected cursor: " + cursor);
        }
        Read apiRead = (Read) read;
        apiRead.ktx.assertOpen();
        ((PartialRelationshipScanCursor) cursor).scan(type, from, to, apiRead);
    }

    public static long[] splitIdIntoPartialSegments(long relationshipHighMark, int numberOfReaders) {
        long threadSize = ParallelUtil.threadSize(numberOfReaders, relationshipHighMark);
        long[] ids = new long[numberOfReaders + 1];
        Arrays.setAll(ids, i -> i * threadSize);
        ids[numberOfReaders] = relationshipHighMark;
        return ids;
    }

    private PartialCursors() {
        throw new UnsupportedOperationException("No instances");
    }
}
