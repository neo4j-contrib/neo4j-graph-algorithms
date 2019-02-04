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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.LongLongMap;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.results.AbstractCommunityResultBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class UnionFindProcExec implements BiConsumer<String, Algorithm<?>> {

    private static final String CONFIG_THRESHOLD = "threshold";
    private static final String CONFIG_CLUSTER_PROPERTY = "partitionProperty";
    private static final String DEFAULT_CLUSTER_PROPERTY = "partition";


    private final GraphDatabaseAPI api;
    private final Log log;
    private final KernelTransaction transaction;
    private final  UnionFindAlgo sequential;
    private final UnionFindAlgo parallel;

    public static Stream<UnionFindResult> run(
            Map<String, Object> config,
            String label,
            String relationship,
            Supplier<UnionFindProcExec> unionFind) {
        ProcedureConfiguration configuration = ProcedureConfiguration
                .create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        AllocationTracker tracker = AllocationTracker.create();

        final Builder builder = new Builder();

        UnionFindProcExec uf = unionFind.get();

        final Graph graph = uf.load(builder::timeLoad, configuration, tracker);

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(UnionFindResult.EMPTY);
        }

        final DSSResult dssResult = uf.evaluate(
                builder::timeEval,
                graph,
                configuration,
                tracker);
        graph.release();

        if (configuration.isWriteFlag()) {
            uf.write(builder::timeWrite, graph, dssResult, configuration);
        }

        if (dssResult.isHuge) {
            return Stream.of(builder.build(graph.nodeCount(), dssResult.hugeStruct::find));
        } else {
            return Stream.of(builder.build(graph.nodeCount(), l -> (long) dssResult.struct.find((int) l)));
        }
    }

    public static class UnionFindResult {

        public static final UnionFindProcExec.UnionFindResult EMPTY = new UnionFindProcExec.UnionFindResult(
                0,
                0,
                0,
                0,
                0,
                0,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1
        );

        public final long loadMillis;
        public final long computeMillis;
        public final long postProcessingMillis;
        public final long writeMillis;
        public final long nodes;
        public final long communityCount;
        public final long setCount;
        public final long p100;
        public final long p99;
        public final long p95;
        public final long p90;
        public final long p75;
        public final long p50;
        public final long p25;
        public final long p10;
        public final long p05;
        public final long p01;

        public UnionFindResult(long loadMillis, long computeMillis, long postProcessingMillis, long writeMillis, long nodes, long communityCount, long p100, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.writeMillis = writeMillis;
            this.nodes = nodes;
            this.communityCount = communityCount;
            this.p100 = p100;
            this.p99 = p99;
            this.p95 = p95;
            this.p90 = p90;
            this.p75 = p75;
            this.p50 = p50;
            this.p25 = p25;
            this.p10 = p10;
            this.p05 = p05;
            this.p01 = p01;
            this.setCount = communityCount;
        }
    }

    public static class Builder extends AbstractCommunityResultBuilder<UnionFindResult> {
        @Override
        protected UnionFindResult build(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodeCount, long communityCount, LongLongMap communitySizeMap, Histogram communityHistogram) {
            return new UnionFindResult(
                    loadMillis,
                    computeMillis,
                    postProcessingMillis,
                    writeMillis,
                    nodeCount,
                    communityCount,
                    communityHistogram.getValueAtPercentile(100),
                    communityHistogram.getValueAtPercentile(99),
                    communityHistogram.getValueAtPercentile(95),
                    communityHistogram.getValueAtPercentile(90),
                    communityHistogram.getValueAtPercentile(75),
                    communityHistogram.getValueAtPercentile(50),
                    communityHistogram.getValueAtPercentile(25),
                    communityHistogram.getValueAtPercentile(10),
                    communityHistogram.getValueAtPercentile(5),
                    communityHistogram.getValueAtPercentile(1)
            );
        }
    }

    public static Stream<DisjointSetStruct.Result> stream(
            Map<String, Object> config,
            String label,
            String relationship,
            Supplier<UnionFindProcExec> unionFind) {
        ProcedureConfiguration configuration = ProcedureConfiguration
                .create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        AllocationTracker tracker = AllocationTracker.create();
        UnionFindProcExec uf = unionFind.get();

        final Graph graph = uf.load(configuration, tracker);

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        DSSResult result = uf.evaluate(graph, configuration, tracker);
        graph.release();
        return result.resultStream(graph);
    }

    public UnionFindProcExec(
            GraphDatabaseAPI api,
            Log log,
            KernelTransaction transaction,
            UnionFindAlgo sequential,
            UnionFindAlgo parallel) {
        this.api = api;
        this.log = log;
        this.transaction = transaction;
        this.sequential = sequential;
        this.parallel = parallel;
    }

    public Graph load(
            Supplier<ProgressTimer> timer,
            ProcedureConfiguration config,
            AllocationTracker tracker) {
        try (ProgressTimer ignored = timer.get()) {
            return load(config, tracker);
        }
    }

    public Graph load(
            ProcedureConfiguration config,
            AllocationTracker tracker) {
        return new GraphLoader(api, Pools.DEFAULT)
                .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                .withOptionalRelationshipWeightsFromProperty(
                        config.getWeightProperty(),
                        config.getWeightPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .withAllocationTracker(tracker)
                .load(config.getGraphImpl());
    }

    private DSSResult evaluate(
            Supplier<ProgressTimer> timer,
            Graph graph,
            ProcedureConfiguration config,
            final AllocationTracker tracker) {
        try (ProgressTimer ignored = timer.get()) {
            return evaluate(graph, config, tracker);
        }
    }

    private DSSResult evaluate(
            Graph graph,
            ProcedureConfiguration config,
            final AllocationTracker tracker) {
        int concurrency = config.getConcurrency();
        int minBatchSize = config.getBatchSize();
        final double threshold = config.get(CONFIG_THRESHOLD, Double.NaN);
        UnionFindAlgo uf = concurrency > 1 ? parallel : sequential;
        return uf.runAny(
                graph,
                Pools.DEFAULT,
                tracker,
                minBatchSize,
                concurrency,
                threshold,
                this);
    }

    private void write(
            Supplier<ProgressTimer> timer,
            Graph graph,
            DSSResult struct,
            ProcedureConfiguration configuration) {
        try (ProgressTimer ignored = timer.get()) {
            write(graph, struct, configuration);
        }
    }

    private void write(
            Graph graph,
            DSSResult struct,
            ProcedureConfiguration configuration) {
        log.debug("Writing results");
        Exporter exporter = Exporter.of(api, graph)
                .withLog(log)
                .parallel(
                        Pools.DEFAULT,
                        configuration.getConcurrency(),
                        TerminationFlag.wrap(transaction))
                .build();
        if (struct.hugeStruct != null) {
            write(exporter, struct.hugeStruct, configuration);
        } else {
            write(exporter, struct.struct, configuration);
        }
    }

    @Override
    public void accept(final String name, final Algorithm<?> algorithm) {
        algorithm.withProgressLogger(ProgressLogger.wrap(log, name))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
    }

    private void write(
            Exporter exporter,
            DisjointSetStruct struct,
            ProcedureConfiguration configuration) {
        exporter.write(
                configuration.get(
                        CONFIG_CLUSTER_PROPERTY,
                        DEFAULT_CLUSTER_PROPERTY),
                struct,
                DisjointSetStruct.Translator.INSTANCE);
    }

    private void write(
            Exporter exporter,
            PagedDisjointSetStruct struct,
            ProcedureConfiguration configuration) {
        exporter.write(
                configuration.get(
                        CONFIG_CLUSTER_PROPERTY,
                        DEFAULT_CLUSTER_PROPERTY),
                struct,
                PagedDisjointSetStruct.Translator.INSTANCE);
    }



}
