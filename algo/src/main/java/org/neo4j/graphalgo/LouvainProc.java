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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.exporter.IntArrayExporter;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.results.LouvainResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Phase 1 Louvain algorithm
 *
 * mandatory parameters:
 *
 *  label: the label or labelQuery
 *  relationship: the relationship or relationshipQuery
 *
 * optional configuration parameters:
 *
 *  weightProperty: relationship weight property name (assumes 1.0 as default if empty)
 *  defaultValue: default weight value if weight property is not set at relationship
 *  write: write flag
 *  writeProperty: name of the property to write result cluster id to
 *  concurrency: concurrency setting (not fully supported yet)
 *
 * @author mknblch
 */
public class LouvainProc {

    public static final String CONFIG_CLUSTER_PROPERTY = "writeProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "community";
    public static final int DEFAULT_ITERATIONS = 10;

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.clustering.louvain", mode = Mode.WRITE)
    @Description("CALL algo.clustering.louvain(label:String, relationship:String, " +
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

        // loading
        final HeavyGraph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = load(configuration);
        }

        builder.withNodeCount(graph.nodeCount());

        final Louvain louvain = new Louvain(
                graph, graph, graph,
                Pools.DEFAULT,
                configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "Louvain"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        // evaluation
        try (ProgressTimer timer = builder.timeEval()) {
            louvain.compute(configuration.getIterations(DEFAULT_ITERATIONS));
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

    @Procedure(value = "algo.clustering.louvain.stream")
    @Description("CALL algo.clustering.louvain.stream(label:String, relationship:String, " +
            "{weightProperty:'propertyName', defaultValue:1.0, concurrency:4) " +
            "YIELD nodeId, community - yields a setId to each node id")
    public Stream<Louvain.Result> louvainStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        // loading
        final HeavyGraph graph = load(configuration);

        // evaluation
        return new Louvain(graph, graph, graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "Louvain"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute(configuration.getIterations(DEFAULT_ITERATIONS))
                .resultStream();

    }

    private HeavyGraph load(ProcedureConfiguration config) {
        return (HeavyGraph) new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(config.getNodeLabelOrQuery())
                .withOptionalRelationshipType(config.getRelationshipOrQuery())
                .withRelationshipWeightsFromProperty(
                        ProcedureConstants.PROPERTY_PARAM,
                        config.getPropertyDefaultValue(1.0))
                .withDirection(Direction.BOTH)
                .load(HeavyGraphFactory.class);
    }

    private void write(Graph graph, int[] communities, ProcedureConfiguration configuration) {
        log.debug("Writing results");
        new IntArrayExporter(
                api,
                graph,
                log,
                configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                Pools.DEFAULT
        ).write(communities);
    }
}
