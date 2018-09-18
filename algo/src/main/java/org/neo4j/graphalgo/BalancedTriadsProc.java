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
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.triangle.*;
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
public class BalancedTriadsProc {

    public static final String DEFAULT_BALANCED_PROPERTY = "balanced";
    public static final String DEFAULT_UNBALANCED_PROPERTY = "unbalanced";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.balancedTriads.stream")
    @Description("CALL algo.balancedTriads.stream(label, relationship, {concurrency:8}) " +
            "YIELD nodeId, balancedTriadCount, unbalancedTriadCount")
    public Stream<HugeBalancedTriads.Result> balancedTriadsStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        // load
        final HugeGraph graph = (HugeGraph) new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withRelationshipWeightsFromProperty(configuration.getWeightProperty(), 0.0)
                .withoutNodeWeights()
                .withSort(true)
                .withLog(log)
                .asUndirected(true)
                .load(configuration.getGraphImpl(HugeGraph.TYPE, HugeGraph.TYPE));

        // omit empty graphs
        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        // compute
        return new HugeBalancedTriads(graph, Pools.DEFAULT, configuration.getConcurrency(), AllocationTracker.create())
                .withProgressLogger(ProgressLogger.wrap(log, "balancedTriads"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute()
                .stream();
    }


    @Procedure(value = "algo.balancedTriads", mode = Mode.WRITE)
    @Description("CALL algo.balancedTriads(label, relationship" +
            "{concurrency:4, write:true, weightProperty:'w', balancedProperty:'balanced', unbalancedProperty:'unbalanced'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, balancedTriadCount, unbalancedTriadCount")
    public Stream<Result> balancedTriads(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final HugeGraph graph;
        final HugeBalancedTriads balancedTriads;

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final BalancedTriadsResultBuilder builder = new BalancedTriadsResultBuilder();

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
            balancedTriads = new HugeBalancedTriads(graph, Pools.DEFAULT, configuration.getConcurrency(), AllocationTracker.create())
                    .withProgressLogger(ProgressLogger.wrap(log, "balancedTriads"))
                    .withTerminationFlag(terminationFlag)
                    .compute();
        }

        // write
        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build()
                        .write(
                                configuration.get("balancedProperty", DEFAULT_BALANCED_PROPERTY),
                                balancedTriads.getBalancedTriangles(),
                                PagedAtomicIntegerArray.Translator.INSTANCE,
                                configuration.get("unbalancedProperty", DEFAULT_UNBALANCED_PROPERTY),
                                balancedTriads.getUnbalancedTriangles(),
                                PagedAtomicIntegerArray.Translator.INSTANCE);
            }
        }

        // result
        return Stream.of(builder.withNodeCount(graph.nodeCount())
                .withBalancedTriadCount(balancedTriads.getBalancedTriangleCount())
                .withUnbalancedTriadCount(balancedTriads.getUnbalancedTriangleCount())
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
        public final long balancedTriadCount;
        public final long unbalancedTriadCount;

        public Result(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long nodeCount, long balancedTriadCount, long unbalancedTriadCount) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodeCount = nodeCount;
            this.balancedTriadCount = balancedTriadCount;
            this.unbalancedTriadCount = unbalancedTriadCount;
        }
    }

    public class BalancedTriadsResultBuilder extends AbstractResultBuilder<Result> {

        private long nodeCount = 0;
        private long balancedTriadCount = 0;
        private long unbalancedTriadCount = 0;

        public BalancedTriadsResultBuilder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public BalancedTriadsResultBuilder withBalancedTriadCount(long balancedTriadCount) {
            this.balancedTriadCount = balancedTriadCount;
            return this;
        }

        public BalancedTriadsResultBuilder withUnbalancedTriadCount(long unbalancedTriadCount) {
            this.unbalancedTriadCount = unbalancedTriadCount;
            return this;
        }

        @Override
        public Result build() {
            return new Result(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount,
                    balancedTriadCount,
                    unbalancedTriadCount);
        }
    }

}
