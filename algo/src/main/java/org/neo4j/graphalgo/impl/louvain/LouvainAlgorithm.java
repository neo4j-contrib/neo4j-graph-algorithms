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
package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.stream.Stream;

/**
 * @author mknblch
 */
public interface LouvainAlgorithm {

    int DEFAULT_ITERATIONS = 5;

    LouvainAlgorithm compute();

    <V> V getCommunityIds();

    int getIterations();

    long getCommunityCount() ;

    Stream<Result> resultStream();

    LouvainAlgorithm withProgressLogger(ProgressLogger progressLogger);

    LouvainAlgorithm withTerminationFlag(TerminationFlag terminationFlag);

    class Result {

        public final long nodeId;
        public final long community;

        public Result(long nodeId, long community) {
            this.nodeId = nodeId;
            this.community = community;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", community=" + community +
                    '}';
        }
    }

    static LouvainAlgorithm instance(Graph graph, ProcedureConfiguration config) {

        if (graph instanceof HugeGraph) {
            if (config.hasWeightProperty()) {
                return new Louvain(graph, config.getIterations(DEFAULT_ITERATIONS), Pools.DEFAULT, config.getConcurrency(), AllocationTracker.create());
            }

            return new HugeParallelLouvain((HugeGraph) graph, Pools.DEFAULT, AllocationTracker.create(), config.getConcurrency(), config.getIterations(DEFAULT_ITERATIONS));
        }

        return new ParallelLouvain(graph,
                graph,
                graph,
                Pools.DEFAULT,
                config.getConcurrency(),
                config.getIterations(DEFAULT_ITERATIONS));
    }
}
