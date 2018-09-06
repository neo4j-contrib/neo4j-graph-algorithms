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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.triangle.HugeBalancedTriads;
import org.neo4j.graphalgo.impl.triangle.HugeLocalTriadDynamics;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class LocalTriadDynamicsProc {

    public static final String DEFAULT_BALANCED_PROPERTY = "balanced";
    public static final String DEFAULT_UNBALANCED_PROPERTY = "unbalanced";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;
    private double probability;

    @Procedure(value = "algo.localTriadDynamics", mode = Mode.WRITE)
    @Description("CALL algo.localTriadDynamics(label, relationship, probability" +
            "{concurrency:4, write:true, weightProperty:'w', balancedProperty:'balanced', unbalancedProperty:'unbalanced'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, balancedTriadCount, unbalancedTriadCount")
    public Stream<Result> balancedTriads(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "probability", defaultValue = "") double probability,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        this.probability = probability;

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
                Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build();
                // TODO .write(...)
            }
        }

        // result
        return Stream.of(builder.withNodeCount(graph.nodeCount())
                .withRebalancedTriadCount(localTriadDynamics.)
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
        public final long rebalancedTriadCount;

        public Result(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long nodeCount, long rebalancedTriadCount) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodeCount = nodeCount;
            this.rebalancedTriadCount = rebalancedTriadCount;
        }
    }

    public class LocalTriadDynamicsResultBuilder extends AbstractResultBuilder<Result> {

        private long nodeCount = 0;
        private long rebalancedTriadCount = 0;

        public LocalTriadDynamicsResultBuilder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public LocalTriadDynamicsResultBuilder withRebalancedTriadCount(long balancedTriadCount) {
            this.rebalancedTriadCount = balancedTriadCount;
            return this;
        }

        @Override
        public Result build() {
            return new Result(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount,
                    rebalancedTriadCount);
        }
    }

}
