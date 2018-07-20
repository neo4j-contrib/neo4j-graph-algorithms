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

import java.lang.invoke.MethodType;

class PartialRelationshipScanCursor extends DefaultRelationshipScanCursor {
    private static final MethodHandle RESET;
    private static final MethodHandle SET_NEXT;

    static {
        MethodHandles.Lookup lookup = PrivateLookup.lookup();
        try {
            RESET = lookup.findVirtual(DefaultRelationshipScanCursor.class, "reset", MethodType.methodType(void.class));
            SET_NEXT = lookup.findSetter(DefaultRelationshipScanCursor.class, "next", long.class);
        } catch (IllegalAccessException | NoSuchMethodException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private long maxId;

    PartialRelationshipScanCursor(DefaultCursors pool) {
        super(pool);
    }

    void scan(int type, long from, long to, Read read) {
        scan(type, read);
        maxId = to;
        if (from > 0L) {
            setNext(from);
        }
    }

    @Override
    public boolean next() {
        if (super.next()) {
            if (getId() < maxId) {
                return true;
            }
            callReset();
        }
        return false;
    }

    private void setNext(long next) {
        try {
            SET_NEXT.invoke(this, next);
        } catch (Throwable throwable) {
            throw ExceptionUtil.asUnchecked(throwable);
        }
    }

    private void callReset() {
        try {
            RESET.invoke(this);
        } catch (Throwable throwable) {
            throw ExceptionUtil.asUnchecked(throwable);
        }
    }
}
