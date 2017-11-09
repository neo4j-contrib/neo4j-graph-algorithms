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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.PageRankResult;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.PageRankAlgorithm;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class PageRankProc {

    public static final String CONFIG_DAMPING = "dampingFactor";

    public static final Double DEFAULT_DAMPING = 0.85;
    public static final Integer DEFAULT_ITERATIONS = 20;
    public static final String DEFAULT_SCORE_PROPERTY = "pagerank";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.pageRank", mode = Mode.WRITE)
    @Description("CALL algo.pageRank(label:String, relationship:String, " +
            "{iterations:5, dampingFactor:0.85, write: true, writeProperty:'pagerank', concurrency:4}) " +
            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty" +
            " - calculates page rank and potentially writes back")
    public Stream<PageRankScore.Stats> pageRank(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder);
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        PageRankResult scores = evaluate(graph, tracker, terminationFlag, configuration, statsBuilder);

        log.info("PageRank: overall memory usage: %s", tracker.getUsageString());

        write(graph, terminationFlag, scores, configuration, statsBuilder);

        return Stream.of(statsBuilder.build());
    }

    @Procedure(value = "algo.pageRank.stream", mode = Mode.READ)
    @Description("CALL algo.pageRank.stream(label:String, relationship:String, " +
            "{iterations:20, dampingFactor:0.85, concurrency:4}) " +
            "YIELD node, score - calculates page rank and streams results")
    public Stream<PageRankScore> pageRankStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder);

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        PageRankResult scores = evaluate(graph, tracker, terminationFlag, configuration, statsBuilder);

        log.info("PageRank: overall memory usage: %s", tracker.getUsageString());

        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            return LongStream.range(0, hugeGraph.nodeCount())
                    .mapToObj(i -> new PageRankScore(
                            api.getNodeById(hugeGraph.toOriginalNodeId(i)),
                            scores.score(i)
                    ));
        }

        return IntStream.range(0, Math.toIntExact(graph.nodeCount()))
                .mapToObj(i -> new PageRankScore(
                        api.getNodeById(graph.toOriginalNodeId(i)),
                        scores.score(i)
                ));
    }

    private Graph load(
            String label,
            String relationship,
            AllocationTracker tracker,
            Class<? extends GraphFactory> graphFactory,
            PageRankScore.Stats.Builder statsBuilder) {

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withAllocationTracker(tracker)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withDirection(Direction.OUTGOING)
                .withoutRelationshipWeights();

        try (ProgressTimer timer = statsBuilder.timeLoad()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodes(graph.nodeCount());
            return graph;
        }
    }

    private PageRankResult evaluate(
            Graph graph,
            AllocationTracker tracker,
            TerminationFlag terminationFlag,
            ProcedureConfiguration configuration,
            PageRankScore.Stats.Builder statsBuilder) {

        double dampingFactor = configuration.get(CONFIG_DAMPING, DEFAULT_DAMPING);
        int iterations = configuration.getIterations(DEFAULT_ITERATIONS);
        final int batchSize = configuration.getBatchSize();
        final int concurrency = configuration.getConcurrency(Pools.getNoThreadsInDefaultPool());
        log.debug("Computing page rank with damping of " + dampingFactor + " and " + iterations + " iterations.");

        PageRankAlgorithm prAlgo = PageRankAlgorithm.of(
                tracker,
                graph,
                dampingFactor,
                Pools.DEFAULT,
                concurrency,
                batchSize);
        Algorithm<?> algo = prAlgo
                .algorithm()
                .withLog(log)
                .withTerminationFlag(terminationFlag);

        statsBuilder.timeEval(() -> prAlgo.compute(iterations));

        statsBuilder
                .withIterations(iterations)
                .withDampingFactor(dampingFactor);

        final PageRankResult pageRank = prAlgo.result();
        algo.release();
        graph.release();
        return pageRank;
    }

    private void write(
            Graph graph,
            TerminationFlag terminationFlag,
            PageRankResult result,
            ProcedureConfiguration configuration,
            final PageRankScore.Stats.Builder statsBuilder) {
        if (configuration.isWriteFlag(true)) {
            log.debug("Writing results");
            String propertyName = configuration.getWriteProperty(DEFAULT_SCORE_PROPERTY);
            try (ProgressTimer timer = statsBuilder.timeWrite()) {
                Exporter exporter = Exporter
                        .of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build();
                result.export(propertyName, exporter);
            }
            statsBuilder
                    .withWrite(true)
                    .withProperty(propertyName);
        } else {
            statsBuilder.withWrite(false);
        }
    }
}
