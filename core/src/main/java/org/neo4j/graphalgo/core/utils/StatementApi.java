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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class StatementApi {

    public interface Consumer<E extends Exception> {
        void accept(Statement statement) throws E;
    }

    public interface Function<T, E extends Exception> {
        T apply(Statement statement) throws E;
    }

    private final GraphDatabaseAPI api;
    private final ThreadToStatementContextBridge contextBridge;

    protected StatementApi(GraphDatabaseAPI api) {
        this.api = api;
        this.contextBridge = api
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    protected <T> T resolve(Class<T> dependency) {
        return api.getDependencyResolver().resolveDependency(dependency);
    }

    public final <T, E extends Exception> T applyInTransaction(Function<T, E> fun)
    throws E {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            final T result = fun.apply(statement);
            tx.success();
            return result;
        }
    }

    public final <E extends Exception> void acceptInTransaction(Consumer<E> fun)
    throws E {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            fun.accept(statement);
            tx.success();
        }
    }
}
