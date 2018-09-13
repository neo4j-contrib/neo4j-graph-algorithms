/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedLongLongBitSet;
import org.neo4j.graphalgo.impl.triangle.HugeLocalTriadDynamics;
import org.neo4j.graphalgo.impl.triangle.RelationExporter;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.neo4j.values.storable.Values;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class LocalTriadDynamicsProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;


    @Procedure(value = "algo.localTriadDynamics", mode = Mode.WRITE)
    @Description("CALL algo.localTriadDynamics(label, relationship, probability" +
            "{concurrency:4, write:true, weightProperty:'w', balancedProperty:'balanced', unbalancedProperty:'unbalanced'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount")
    public Stream<Result> balancedTriads(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "p", defaultValue = "0.5") double probability,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final HugeGraph graph;
        final HugeLocalTriadDynamics localTriadDynamics;

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final LocalTriadDynamicsResultBuilder builder = new LocalTriadDynamicsResultBuilder();

        // load
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = (HugeGraph) new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withRelationshipWeightsFromProperty(configuration.getWeightProperty(), 0.0)
                    .withoutNodeWeights()
                    .withSort(true)
                    .withLog(log)
                    .asUndirected(true)
                    .load(configuration.getGraphImpl(HugeGraph.TYPE, HugeGraph.TYPE));
        }

        // compute
        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        try (ProgressTimer timer = builder.timeEval()) {
            localTriadDynamics = new HugeLocalTriadDynamics(graph, Pools.DEFAULT, configuration.getConcurrency(), AllocationTracker.create())
                    .withProgressLogger(ProgressLogger.wrap(log, "balancedTriads"))
                    .withTerminationFlag(terminationFlag)
                    .compute(probability);
        }

        // write
        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                new RelationExporter(api, configuration.getWeightProperty())
                        .export((s, t) -> Values.doubleValue(localTriadDynamics.getWeight(s, t)));
            }
        }

        // release
        localTriadDynamics.release();
        return Stream.of(builder.withNodeCount(graph.nodeCount())
                .build());
    }

    /**
     * result dto
     */
    public static class Result {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long nodeCount;

        public Result(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long nodeCount) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodeCount = nodeCount;
        }
    }

    public class LocalTriadDynamicsResultBuilder extends AbstractResultBuilder<Result> {

        private long nodeCount = 0;

        public LocalTriadDynamicsResultBuilder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        @Override
        public Result build() {
            return new Result(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount);
        }
    }

}
