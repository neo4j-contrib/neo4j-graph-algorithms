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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class GraphDimensions extends StatementTask<GraphDimensions, RuntimeException> {
    private final GraphSetup setup;

    private long nodeCount;
    private long allNodesCount;
    private long maxRelCount;
    private int labelId;
    private int[] relationId;
    private int weightId;
    private int relWeightId;
    private int nodeWeightId;
    private int nodePropId;

    public GraphDimensions(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api);
        this.setup = setup;
    }

    public long hugeNodeCount() {
        return nodeCount;
    }

    public long allNodesCount() {
        return allNodesCount;
    }

    public int nodeCount() {
        return Math.toIntExact(nodeCount);
    }

    public long maxRelCount() {
        return maxRelCount;
    }

    public int labelId() {
        return labelId;
    }

    public int[] relationId() {
        return relationId;
    }

    public int weightId() {
        return weightId;
    }

    public int relWeightId() {
        return relWeightId;
    }

    public int nodeWeightId() {
        return nodeWeightId;
    }

    public int nodePropId() {
        return nodePropId;
    }

    @Override
    public GraphDimensions apply(final Statement statement) throws RuntimeException {
        final ReadOperations readOp = statement.readOperations();
        labelId = setup.loadAnyLabel()
                ? ReadOperations.ANY_LABEL
                : readOp.labelGetForName(setup.startLabel);
        if (!setup.loadAnyRelationshipType()) {
            int relId = readOp.relationshipTypeGetForName(setup.relationshipType);
            if (relId != StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                relationId = new int[]{relId};
            }
        }
        weightId = setup.loadDefaultRelationshipWeight()
                ? StatementConstants.NO_SUCH_PROPERTY_KEY
                : readOp.propertyKeyGetForName(setup.relationWeightPropertyName);
        relWeightId = setup.loadDefaultRelationshipWeight()
                ? StatementConstants.NO_SUCH_PROPERTY_KEY
                : readOp.propertyKeyGetForName(setup.relationWeightPropertyName);
        nodeWeightId = setup.loadDefaultNodeWeight()
                ? StatementConstants.NO_SUCH_PROPERTY_KEY
                : readOp.propertyKeyGetForName(setup.nodeWeightPropertyName);
        nodePropId = setup.loadDefaultNodeProperty()
                ? StatementConstants.NO_SUCH_PROPERTY_KEY
                : readOp.propertyKeyGetForName(setup.nodePropertyName);
        nodeCount = readOp.countsForNode(labelId);
        allNodesCount = getHighestPossibleNodeCount(readOp);
        maxRelCount = Math.max(
                readOp.countsForRelationshipWithoutTxState(
                        labelId,
                        relationId == null ? ReadOperations.ANY_RELATIONSHIP_TYPE : relationId[0],
                        ReadOperations.ANY_LABEL),
                readOp.countsForRelationshipWithoutTxState(
                        ReadOperations.ANY_LABEL,
                        relationId == null ? ReadOperations.ANY_RELATIONSHIP_TYPE : relationId[0],
                        labelId)
        );
        return this;
    }

    private long getHighestPossibleNodeCount(ReadOperations readOp) {
        try {
            IdGeneratorFactory idGeneratorFactory = resolve(IdGeneratorFactory.class);
            if (idGeneratorFactory != null) {
                final IdGenerator idGenerator = idGeneratorFactory.get(IdType.NODE);
                if (idGenerator != null) {
                    return idGenerator.getHighId();
                }
            }
        } catch (IllegalArgumentException | UnsatisfiedDependencyException ignored) {
        }
        return readOp.nodesGetCount();
    }
}
