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

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.louvain.*;
import org.neo4j.graphalgo.results.LouvainResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * modularity based community detection algorithm
 *
 * @author mknblch
 */
public class LouvainProc {

    public static final String CONFIG_CLUSTER_PROPERTY = "writeProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "community";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.louvain", mode = Mode.WRITE)
    @Description("CALL algo.louvain(label:String, relationship:String, " +
            "{weightProperty:'weight', defaultValue:1.0, write: true, writeProperty:'community', concurrency:4}) " +
            "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis")
    public Stream<LouvainResult> louvain(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        LouvainResult.Builder builder = LouvainResult.builder();

        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = graph(configuration);
        }

        builder.withNodeCount(graph.nodeCount());

        final LouvainAlgorithm louvain = LouvainAlgorithm.instance(graph, configuration)
                .withProgressLogger(ProgressLogger.wrap(log, "Louvain"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        // evaluation
        try (ProgressTimer timer = builder.timeEval()) {
            louvain.compute();
            builder.withIterations(louvain.getIterations())
                    .withCommunityCount(louvain.getCommunityCount());
        }

        if (configuration.isWriteFlag()) {
            // write back
            builder.timeWrite(() ->
                    write(graph, louvain.getCommunityIds(), configuration));
        }

        return Stream.of(builder.build());
    }

    @Procedure(value = "algo.louvain.stream")
    @Description("CALL algo.louvain.stream(label:String, relationship:String, " +
            "{weightProperty:'propertyName', defaultValue:1.0, concurrency:4) " +
            "YIELD nodeId, community - yields a setId to each node id")
    public Stream<WeightedLouvain.Result> louvainStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        // evaluation
        return LouvainAlgorithm.instance(graph(configuration), configuration)
                .withProgressLogger(ProgressLogger.wrap(log, "Louvain"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute()
                .resultStream();

    }

    public Graph graph(ProcedureConfiguration config) {

        final Class<? extends GraphFactory> graphImpl =
                config.getGraphImpl(HugeGraph.TYPE,
                        HeavyGraph.TYPE, HeavyCypherGraphFactory.TYPE, HugeGraph.TYPE);

        final GraphLoader loader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                .asUndirected(true);

        if (config.hasWeightProperty()) {
            return loader
                    .withOptionalRelationshipWeightsFromProperty(
                            config.getWeightProperty(),
                            config.getWeightPropertyDefaultValue(1.0))
                    .load(graphImpl);
        }

        return loader
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .withoutNodeProperties()
                .load(graphImpl);
    }

    private void write(Graph graph, Object communities, ProcedureConfiguration configuration) {
        log.debug("Writing results");
        final Exporter exporter = Exporter.of(api, graph)
                .withLog(log)
                .parallel(Pools.DEFAULT, configuration.getConcurrency(), TerminationFlag.wrap(transaction))
                .build();

        if (communities instanceof int[]) {
            exporter.write(
                    configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                    (int[]) communities,
                    Translators.INT_ARRAY_TRANSLATOR);
        } else if (communities instanceof HugeLongArray) {
            exporter.write(
                    configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                    (HugeLongArray) communities,
                    HugeLongArray.Translator.INSTANCE);
        }
    }
}
