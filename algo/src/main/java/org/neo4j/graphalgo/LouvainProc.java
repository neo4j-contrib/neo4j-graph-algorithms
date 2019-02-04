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

import com.carrotsearch.hppc.LongLongMap;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.louvain.*;
import org.neo4j.graphalgo.results.AbstractCommunityResultBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * modularity based community detection algorithm
 *
 * @author mknblch
 */
public class LouvainProc {

    public static final String INTERMEDIATE_COMMUNITIES_WRITE_PROPERTY = "intermediateCommunitiesWriteProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "communityProperty";
    public static final String INCLUDE_INTERMEDIATE_COMMUNITIES = "includeIntermediateCommunities";

    private static final String CLUSTERING_IDENTIFIER = "clustering";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.louvain", mode = Mode.WRITE)
    @Description("CALL algo.louvain(label:String, relationship:String, " +
            "{weightProperty:'weight', defaultValue:1.0, write: true, writeProperty:'community', concurrency:4, communityProperty:'propertyOfPredefinedCommunity'}) " +
            "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis")
    public Stream<LouvainResult> louvain(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);


        final Builder builder = new Builder();

        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = graph(label, relationship, configuration);
        }

        if(graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(LouvainResult.EMPTY);
        }

        final Louvain louvain = new Louvain(graph, Pools.DEFAULT, configuration.getConcurrency(), AllocationTracker.create())
                .withProgressLogger(ProgressLogger.wrap(log, "Louvain"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        // evaluation
        int iterations = configuration.getIterations(10);
        try (ProgressTimer timer = builder.timeEval()) {
            if (configuration.getString(DEFAULT_CLUSTER_PROPERTY).isPresent()) {
                // use predefined clustering
                final WeightMapping communityMap = ((NodeProperties) graph).nodeProperties(CLUSTERING_IDENTIFIER);
                louvain.compute(communityMap, iterations, configuration.get("innerIterations", 10));
            } else {
                louvain.compute(iterations, configuration.get("innerIterations", 10));
            }
        }

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> write(graph, louvain.getDendrogram(), louvain.getCommunityIds(), configuration));
        }

        builder.withIterations(louvain.getLevel());
        builder.withModularities(louvain.getModularities()  );
        builder.withFinalModularity(louvain.getFinalModularity());

        final int[] communityIds = louvain.getCommunityIds();
        return Stream.of(builder.build(graph.nodeCount(), n -> (long) communityIds[(int) n]));
    }

    @Procedure(value = "algo.louvain.stream")
    @Description("CALL algo.louvain.stream(label:String, relationship:String, " +
            "{weightProperty:'propertyName', defaultValue:1.0, concurrency:4, communityProperty:'propertyOfPredefinedCommunity') " +
            "YIELD nodeId, community - yields a setId to each node id")
    public Stream<Louvain.StreamingResult> louvainStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final Graph graph = graph(label, relationship, configuration);

        // evaluation
        final Louvain louvain = new Louvain(graph, Pools.DEFAULT, configuration.getConcurrency(), AllocationTracker.create())
                .withProgressLogger(ProgressLogger.wrap(log, "Louvain"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        if (configuration.getString(DEFAULT_CLUSTER_PROPERTY).isPresent()) {
            // use predefined clustering
            final WeightMapping communityMap = ((NodeProperties) graph).nodeProperties(CLUSTERING_IDENTIFIER);
            louvain.compute(communityMap, configuration.getIterations(10), configuration.get("innerIterations", 10));
        } else {
            louvain.compute(configuration.getIterations(10), configuration.get("innerIterations", 10));
        }

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        return louvain.dendrogramStream(configuration.get(INCLUDE_INTERMEDIATE_COMMUNITIES, false));
    }

    public Graph graph(String label, String relationship, ProcedureConfiguration config) {

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, config)
                .withNodeStatement(config.getNodeLabelOrQuery())
                .withRelationshipStatement(config.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(config.getWeightProperty(), config.getWeightPropertyDefaultValue(1.0));

        config.getString(DEFAULT_CLUSTER_PROPERTY).ifPresent(propertyIdentifier -> {
            // configure predefined clustering if set
            graphLoader.withOptionalNodeProperties(PropertyMapping.of(CLUSTERING_IDENTIFIER, propertyIdentifier, -1));
        });

        return graphLoader
                .asUndirected(true)
                .load(config.getGraphImpl());
    }

    private void write(Graph graph, int[][] allCommunities, int[] finalCommunities, ProcedureConfiguration configuration) {
        log.debug("Writing results");
        boolean includeIntermediateCommunities = configuration.get(INCLUDE_INTERMEDIATE_COMMUNITIES, false);

        new LouvainCommunityExporter(
                api,
                Pools.DEFAULT,
                configuration.getConcurrency(),
                graph,
                finalCommunities.length,
                configuration.getWriteProperty("community"),
                configuration.get(INTERMEDIATE_COMMUNITIES_WRITE_PROPERTY, "communities"))
                .export(allCommunities, finalCommunities, includeIntermediateCommunities);
    }

    public static class LouvainResult {

        public static final LouvainResult EMPTY = new LouvainResult(
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
                -1,
                0,
                new double[] {}, -1);

        public final long loadMillis;
        public final long computeMillis;
        public final long postProcessingMillis;
        public final long writeMillis;
        public final long nodes;
        public final long communityCount;
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
        public final long iterations;
        public final List<Double> modularities;
        public final double modularity;

        public LouvainResult(long loadMillis, long computeMillis, long postProcessingMillis, long writeMillis, long nodes, long communityCount, long p100, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01, long iterations, double[] modularities, double finalModularity) {
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
            this.iterations = iterations;
            this.modularities = new ArrayList<>(modularities.length);
            for (double mod : modularities) this.modularities.add(mod);
            this.modularity = finalModularity;
        }
    }

    public static class Builder extends AbstractCommunityResultBuilder<LouvainResult> {

        private long iterations = -1;
        private double[] modularities = new double[] {};
        private double finalModularity = -1;

        public Builder withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        @Override
        protected LouvainResult build(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodeCount, long communityCount, LongLongMap communitySizeMap, Histogram communityHistogram) {
            return new LouvainResult(
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
                    communityHistogram.getValueAtPercentile(1),
                    iterations, modularities, finalModularity
            );
        }

        public Builder withModularities(double[] modularities) {
            this.modularities = modularities;
            return this;
        }

        public Builder withFinalModularity(double finalModularity) {
            this.finalModularity = finalModularity;
            return null;
        }
    }


}
