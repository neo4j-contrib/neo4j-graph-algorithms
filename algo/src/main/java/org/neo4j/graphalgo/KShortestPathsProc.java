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

import com.carrotsearch.hppc.IntArrayDeque;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.ShortestPathAStar;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphalgo.results.DijkstraResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class KShortestPathsProc {

    public static final String DEFAULT_TARGET_PROPERTY = "sssp";


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.kShortestPatha", mode = Mode.WRITE)
    @Description("CALL algo.shortestPath(startNode:Node, endNode:Node, k:int, weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', direction:'BOTH', defaultValue:1.0, write:'true', writeProperty:'sssp'}) " +
            "YIELD nodeId, cost, loadMillis, evalMillis, writeMillis - yields nodeCount, totalCost, loadMillis, evalMillis, writeMillis")
    public Stream<DijkstraResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        DijkstraResult.Builder builder = DijkstraResult.builder();

        final Graph graph;
        final YensKShortestPaths algorithm;

        Direction direction = configuration.getDirection(Direction.BOTH);
        try (ProgressTimer timer = builder.timeLoad()) {
            final GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                    .withOptionalRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getWeightPropertyDefaultValue(1.0));
            if (direction == Direction.BOTH) {
                direction = Direction.OUTGOING; // rewrite
                graphLoader.asUndirected(true);
            } else {
                graphLoader.withDirection(direction);
            }
            graph = graphLoader.load(configuration.getGraphImpl());
        }

        try (ProgressTimer timer = builder.timeEval()) {
            algorithm = new YensKShortestPaths(graph)
                    .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(Dijkstra)"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute(startNode.getId(), endNode.getId(), direction, (int) k);
        }

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {

//                algorithm.getPaths()
                // TODO
            }
        }

        return Stream.of(builder.build());
    }
}
