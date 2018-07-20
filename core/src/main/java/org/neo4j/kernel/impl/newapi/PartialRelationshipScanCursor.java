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

import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.PrivateLookup;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

class PartialRelationshipScanCursor extends DefaultRelationshipScanCursor {
    private static final MethodHandle SET_NEXT;
    private static final MethodHandle SET_TYPE;
    private static final MethodHandle SET_HIGH_MARK;

    static {
        MethodHandles.Lookup lookup = PrivateLookup.lookup();
        try {
            SET_NEXT = lookup.findSetter(DefaultRelationshipScanCursor.class, "next", long.class);
            SET_TYPE = lookup.findSetter(DefaultRelationshipScanCursor.class, "type", int.class);
            SET_HIGH_MARK = lookup.findSetter(DefaultRelationshipScanCursor.class, "highMark", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private long maxId;

    PartialRelationshipScanCursor(DefaultCursors pool) {
        super(pool);
    }

    void scan(int type, long from, long to, Read read) {
        single(from, read);
        maxId = to;
        try {
            SET_TYPE.invoke(this, type);
            SET_HIGH_MARK.invoke(this, Math.min(to, read.relationshipHighMark()));
        } catch (Throwable throwable) {
            throw ExceptionUtil.asUnchecked(throwable);
        }
    }

    @Override
    public boolean next() {
        boolean hasNext = super.next();
        if (hasNext && getId() >= maxId) {
            setEmptyNext();
            return false;
        }
        return hasNext;
    }

    private void setEmptyNext() {
        try {
            SET_NEXT.invoke(this, (long) NO_ID);
        } catch (Throwable throwable) {
            throw ExceptionUtil.asUnchecked(throwable);
        }
    }
}
