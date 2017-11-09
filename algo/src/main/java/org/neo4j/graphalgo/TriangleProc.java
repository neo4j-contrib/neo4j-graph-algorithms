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
import org.neo4j.graphalgo.exporter.AtomicIntArrayExporter;
import org.neo4j.graphalgo.exporter.TriangleExporter;
import org.neo4j.graphalgo.impl.TriangleCount;
import org.neo4j.graphalgo.impl.TriangleCountExp;
import org.neo4j.graphalgo.impl.TriangleStream;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class TriangleProc {

    public static final String DEFAULT_WRITE_PROPERTY_VALUE = "triangles";
    public static final String COEFFICIENT_WRITE_PROPERTY_VALUE = "clusteringCoefficientProperty";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.triangle.stream")
    @Description("CALL algo.triangle.stream(label, relationship, {concurrency:4}) " +
            "YIELD nodeA, nodeB, nodeC - yield nodeA, nodeB and nodeC which form a triangle")
    public Stream<TriangleStream.Result> triangleStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .withLog(log)
                .withDirection(TriangleCount.D)
                .load(configuration.getGraphImpl());

        final TriangleStream triangleStream = new TriangleStream(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "triangleStream"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        return triangleStream.resultStream();
    }

    @Procedure("algo.triangleCount.exp1.stream")
    @Description("CALL algo.triangleCount.exp1.stream(label, relationship, {concurrency:4}) " +
            "YIELD nodeId, triangles - yield nodeId, number of triangles")
    public Stream<TriangleCount.Result> triangleCountStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .withLog(log)
                .withDirection(TriangleCount.D)
                .load(configuration.getGraphImpl());

        final TriangleCount triangleCount = new TriangleCount(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute();

        return triangleCount.resultStream();
    }

    @Procedure("algo.triangleCount.stream")
    @Description("CALL algo.triangleCount.stream(label, relationship, {concurrency:8}) " +
            "YIELD nodeId, triangles - yield nodeId, number of triangles")
    public Stream<TriangleCountExp.Result> triangleCountExp1Stream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .withLog(log)
                .withDirection(TriangleCount.D)
                .load(configuration.getGraphImpl());

        final TriangleCountExp triangleCount = new TriangleCountExp(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute();

        return triangleCount.resultStream();
    }

    @Procedure(value = "algo.triangleCount.exp1", mode = Mode.WRITE)
    @Description("CALL algo.triangleCount.exp1(label, relationship, " +
            "{concurrency:4, write:true, writeProperty:'triangles', clusteringCoefficientProperty:'coefficient'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount, averageClusteringCoefficient")
    public Stream<Result> triangleCount(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final Graph graph;
        final TriangleCount triangleCount;
        final double[] clusteringCoefficients;

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);
        final TriangleCountResultBuilder builder = new TriangleCountResultBuilder();

        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .withLog(log)
                    .withDirection(TriangleCount.D)
                    .load(configuration.getGraphImpl());
        };

        try (ProgressTimer timer = builder.timeEval()) {
            triangleCount = new TriangleCount(graph, Pools.DEFAULT, configuration.getConcurrency())
                    .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
            clusteringCoefficients = triangleCount.getClusteringCoefficients();
        };

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                final Optional<String> coefficientProperty = configuration.getString(COEFFICIENT_WRITE_PROPERTY_VALUE);
                if (coefficientProperty.isPresent()) {
                    new TriangleExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE))
                            .setCoefficientWriteProperty(coefficientProperty.get())
                            .write(triangleCount.getTriangles(), clusteringCoefficients);
                } else {
                    new AtomicIntArrayExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE))
                            .write(triangleCount.getTriangles());
                }

            }
        }

        builder.withNodeCount(graph.nodeCount())
                .withTriangleCount(triangleCount.getTriangleCount())
                .withAverageClusteringCoefficient(triangleCount.getAverageClusteringCoefficient());

        return Stream.of(builder.build());
    }

    @Procedure(value = "algo.triangleCount", mode = Mode.WRITE)
    @Description("CALL algo.triangleCount(label, relationship, " +
            "{concurrency:8, write:true, writeProperty:'triangles', clusteringCoefficientProperty:'coefficient'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount, averageClusteringCoefficient")
    public Stream<Result> triangleCountExp1(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final Graph graph;
        final TriangleCountExp triangleCount;
        final double[] clusteringCoefficients;

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);
        final TriangleCountResultBuilder builder = new TriangleCountResultBuilder();

        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .withLog(log)
                    .withDirection(TriangleCount.D)
                    .load(configuration.getGraphImpl());
        };

        try (ProgressTimer timer = builder.timeEval()) {
            triangleCount = new TriangleCountExp(graph, Pools.DEFAULT, configuration.getConcurrency())
                    .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
            clusteringCoefficients = triangleCount.getClusteringCoefficients();
        };

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                final Optional<String> coefficientProperty = configuration.getString(COEFFICIENT_WRITE_PROPERTY_VALUE);
                if (coefficientProperty.isPresent()) {
                    new TriangleExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE))
                            .setCoefficientWriteProperty(coefficientProperty.get())
                            .write(triangleCount.getTriangles(), clusteringCoefficients);
                } else {
                    new AtomicIntArrayExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE))
                            .write(triangleCount.getTriangles());
                }

            }
        }

        builder.withNodeCount(graph.nodeCount())
                .withTriangleCount(triangleCount.getTriangleCount())
                .withAverageClusteringCoefficient(triangleCount.getAverageClusteringCoefficient());

        return Stream.of(builder.build());
    }

    /**
     * result dto
     */
    public static class Result {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long nodeCount;
        public final long triangleCount;
        public final double averageClusteringCoefficient;

        public Result(long loadMillis, long computeMillis, long writeMillis, long nodeCount, long triangleCount, double averageClusteringCoefficient) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodeCount = nodeCount;
            this.triangleCount = triangleCount;
            this.averageClusteringCoefficient = averageClusteringCoefficient;
        }
    }

    public class TriangleCountResultBuilder extends AbstractResultBuilder<Result> {

        private long nodeCount = -1L;
        private long triangleCount = -1L;
        private double averageClusteringCoefficient = -1d;

        public TriangleCountResultBuilder withAverageClusteringCoefficient(double averageClusteringCoefficient) {
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            return this;
        }

        public TriangleCountResultBuilder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public TriangleCountResultBuilder withTriangleCount(long triangleCount) {
            this.triangleCount = triangleCount;
            return this;
        }

        @Override
        public Result build() {
            return new Result(loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount,
                    triangleCount,
                    averageClusteringCoefficient);
        }
    }

}
