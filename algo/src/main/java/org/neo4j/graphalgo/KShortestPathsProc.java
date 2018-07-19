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
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.yens.WeightedPathExporter;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Yen's K shortest paths algorithm. Computes multiple shortest
 * paths from a given start to a goal node in the desired direction.
 * The paths are written to the graph using new relationships named
 * by prefix + index.
 *
 * @author mknblch
 */
public class KShortestPathsProc {

    public static final String DEFAULT_TARGET_PROPERTY = "PATH_";
    public static final String PREFIX_IDENTIFIER = "writePropertyPrefix";
    public static final String REL_TYPE_PROPERTY_IDENTIFIER = "writeRelationshipTypeProperty";
    public static final String DEFAULT_RELATIONSHIP_PROPERTY = "weight";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.kShortestPaths", mode = Mode.WRITE)
    @Description("CALL algo.kShortestPaths(startNode:Node, endNode:Node, k:int, weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', direction:'OUT', defaultValue:1.0, maxDepth:42, write:'true', " +
            PREFIX_IDENTIFIER + ":'PATH_'}) " +
            "YIELD resultCount, loadMillis, evalMillis, writeMillis - yields resultCount, loadMillis, evalMillis, writeMillis")
    public Stream<KspResult> yens(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final KspResult.Builder builder = new KspResult.Builder();
        final Graph graph;
        final YensKShortestPaths algorithm;
        Direction direction = configuration.getDirection(Direction.BOTH);
        // load
        try (ProgressTimer timer = builder.timeLoad()) {
            final GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                    .withOptionalRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getWeightPropertyDefaultValue(1.0));
            // use undirected traversal if direction is BOTH
            if (direction == Direction.BOTH) {
                direction = Direction.OUTGOING; // rewrite
                graphLoader.asUndirected(true);
            } else {
                graphLoader.withDirection(direction);
            }
            graph = graphLoader.load(configuration.getGraphImpl());
        }

        if (graph.nodeCount() == 0 || startNode == null || endNode == null) {
            graph.release();
            return Stream.of(builder.build());
        }

        // eval
        try (ProgressTimer timer = builder.timeEval()) {
            algorithm = new YensKShortestPaths(graph)
                    .withProgressLogger(ProgressLogger.wrap(log, "KShortestPaths(Yen)"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute(startNode.getId(),
                            endNode.getId(),
                            direction,
                            Math.toIntExact(k),
                            configuration.getNumber("maxDepth", Integer.MAX_VALUE).intValue());
            builder.withResultCount(algorithm.getPaths().size());
        }
        // write
        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                new WeightedPathExporter(api,
                        Pools.DEFAULT,
                        graph,
                        graph,
                        configuration.getString(PREFIX_IDENTIFIER, DEFAULT_TARGET_PROPERTY),
                        configuration.getString(REL_TYPE_PROPERTY_IDENTIFIER, DEFAULT_RELATIONSHIP_PROPERTY))
                        .export(algorithm.getPaths());
            }
        }
        return Stream.of(builder.build());
    }

    public static class KspResult {

        public final long loadMillis;
        public final long evalMillis;
        public final long writeMillis;
        public final long resultCount;

        public KspResult(long loadMillis, long evalMillis, long writeMillis, long resultCount) {
            this.loadMillis = loadMillis;
            this.evalMillis = evalMillis;
            this.writeMillis = writeMillis;
            this.resultCount = resultCount;
        }

        public static class Builder extends AbstractResultBuilder<KspResult> {

            private int resultCount;

            public Builder withResultCount(int resultCount) {
                this.resultCount = resultCount;
                return this;
            }

            @Override
            public KspResult build() {
                return new KspResult(
                        this.loadDuration,
                        this.evalDuration,
                        this.writeDuration,
                        resultCount);
            }
        }
    }
}
