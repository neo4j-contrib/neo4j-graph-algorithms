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
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author mknblch
 */
public abstract class AbstractExporter<T> {

    protected final GraphDatabaseAPI api;

    protected final ThreadToStatementContextBridge bridge;

    public AbstractExporter(GraphDatabaseAPI api) {
        this.api = api;
        this.bridge = api
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    public abstract void write(T data);

    protected int getOrCreatePropertyId(String propertyName) {
        return tokenWriteInTx(tokenWriteOperations -> {
            try {
                return tokenWriteOperations
                        .propertyKeyGetOrCreateForName(propertyName);
            } catch (IllegalTokenNameException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected int getOrCreateRelationshipId(String relationshipName) {
        return tokenWriteInTx(tokenWriteOperations -> {
            try {
                return tokenWriteOperations
                        .relationshipTypeGetOrCreateForName(relationshipName);
            } catch (IllegalTokenNameException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected void readInTransaction(Consumer<ReadOperations> consumer) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            consumer.accept(statement.readOperations());
            tx.success();
        }
    }

    protected void writeInTransaction(Consumer<DataWriteOperations> consumer) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            consumer.accept(statement.dataWriteOperations());
            tx.success();
        } catch (InvalidTransactionTypeKernelException e) {
            throw new RuntimeException(e);
        }
    }

    protected <V> V tokenWriteInTx(Function<TokenWriteOperations, V> consumer) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            V v = consumer.apply(statement.tokenWriteOperations());
            tx.success();
            return v;
        }
    }
}
