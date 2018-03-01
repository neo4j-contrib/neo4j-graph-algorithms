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
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.core.utils.paged.DoubleArray;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.triangle.*;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicIntegerArray;
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
                .withSort(true)
                .asUndirected(true)
                .init(log, label, relationship, configuration)
                .withDirection(TriangleCountBase.D)
                .load(configuration.getGraphImpl(
                        HeavyGraph.TYPE,
                        HeavyGraph.TYPE, HeavyCypherGraphFactory.TYPE, HugeGraph.TYPE
                ));

        final TriangleStream triangleStream = new TriangleStream(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "triangleStream"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        return triangleStream.resultStream();
    }

    @Procedure("algo.triangleCount.stream")
    @Description("CALL algo.triangleCount.stream(label, relationship, {concurrency:8}) " +
            "YIELD nodeId, triangles - yield nodeId, number of triangles")
    public Stream<TriangleCountAlgorithm.Result> triangleCountQueueStream(
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
                .withSort(true)
                .asUndirected(true)
                .init(log, label, relationship, configuration)
                .withDirection(TriangleCountBase.D)
                .load(configuration.getGraphImpl(
                        HeavyGraph.TYPE,
                        HeavyGraph.TYPE, HeavyCypherGraphFactory.TYPE, HugeGraph.TYPE
                ));

        return TriangleCountAlgorithm.instance(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute()
                .resultStream();
    }


    @Procedure("algo.triangleCount.forkJoin.stream")
    @Description("CALL algo.triangleCount.forkJoin.stream(label, relationship, {concurrency:8}) " +
            "YIELD nodeId, triangles - yield nodeId, number of triangles")
    public Stream<TriangleCountBase.Result> triangleCountForkJoinStream(
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
                .withSort(true)
                .asUndirected(true)
                .init(log, label, relationship, configuration)
                .withDirection(TriangleCountBase.D)
                .load(configuration.getGraphImpl(
                        HeavyGraph.TYPE,
                        HeavyGraph.TYPE, HeavyCypherGraphFactory.TYPE, HugeGraph.TYPE
                ));

        return new TriangleCountForkJoin(
                graph,
                ForkJoinPool.commonPool(),
                configuration.getNumber("threshold", 10_000).intValue())
                .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute()
                .resultStream();
    }


    @Procedure(value = "algo.triangleCount", mode = Mode.WRITE)
    @Description("CALL algo.triangleCount(label, relationship, " +
            "{concurrency:4, write:true, writeProperty:'triangles', clusteringCoefficientProperty:'coefficient'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount, averageClusteringCoefficient")
    public Stream<Result> triangleCountQueue(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final Graph graph;
        final TriangleCountAlgorithm triangleCount;
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
                    .withSort(true)
                    .asUndirected(true)
                    .init(log, label, relationship, configuration)
                    .withDirection(TriangleCountBase.D)
                    .load(configuration.getGraphImpl(
                            HeavyGraph.TYPE,
                            HeavyGraph.TYPE, HeavyCypherGraphFactory.TYPE, HugeGraph.TYPE
                    ));
        }

        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        try (ProgressTimer timer = builder.timeEval()) {
            triangleCount = TriangleCountAlgorithm.instance(graph, Pools.DEFAULT, configuration.getConcurrency())
                    .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
            clusteringCoefficients = triangleCount.getCoefficients();
        }

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                write(graph, triangleCount, configuration, terminationFlag);
            }
        }

        builder.withNodeCount(graph.nodeCount())
                .withTriangleCount(triangleCount.getTriangleCount())
                .withAverageClusteringCoefficient(triangleCount.getAverageCoefficient());

        return Stream.of(builder.build());
    }

    /**
     * writeback method for "algo.triangleCount"
     * @param graph the graph
     * @param algorithm Impl. of TriangleCountAlgorithm
     * @param configuration configuration wrapper
     * @param flag termination flag
     */
    private void write(Graph graph, TriangleCountAlgorithm algorithm, ProcedureConfiguration configuration, TerminationFlag flag) {

        final Optional<String> coefficientProperty = configuration.getString(COEFFICIENT_WRITE_PROPERTY_VALUE);

        final Exporter exporter = Exporter.of(api, graph)
                .withLog(log)
                .parallel(Pools.DEFAULT, configuration.getConcurrency(), flag)
                .build();

        if (algorithm instanceof HugeTriangleCount) {
            if (coefficientProperty.isPresent()) {
                // huge with coefficients
                final DoubleArray coefficients = ((HugeTriangleCount) algorithm).getCoefficients();
                final PagedAtomicIntegerArray triangles = ((HugeTriangleCount) algorithm).getTriangles();
                exporter.write(
                        configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE),
                        triangles,
                        PagedAtomicIntegerArray.Translator.INSTANCE,
                        coefficientProperty.get(),
                        coefficients,
                        DoubleArray.Translator.INSTANCE
                );
            } else {
                // huge without coefficients
                final PagedAtomicIntegerArray triangles = ((HugeTriangleCount) algorithm).getTriangles();
                exporter.write(
                        configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE),
                        triangles,
                        PagedAtomicIntegerArray.Translator.INSTANCE
                );
            }
        } else if (algorithm instanceof TriangleCountQueue){

            if (coefficientProperty.isPresent()) {
                // nonhuge with coefficients
                final double[] coefficients = ((TriangleCountQueue) algorithm).getCoefficients();
                final AtomicIntegerArray triangles = ((TriangleCountQueue) algorithm).getTriangles();
                exporter.write(
                        configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE),
                        triangles,
                        Translators.ATOMIC_INTEGER_ARRAY_TRANSLATOR,
                        coefficientProperty.get(),
                        coefficients,
                        Translators.DOUBLE_ARRAY_TRANSLATOR
                );
            } else {
                // nonhuge without coefficients
                final AtomicIntegerArray triangles = ((TriangleCountQueue) algorithm).getTriangles();
                exporter.write(
                        configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE),
                        triangles,
                        Translators.ATOMIC_INTEGER_ARRAY_TRANSLATOR
                );
            }
        }
    }

    @Procedure(value = "algo.triangleCount.forkJoin", mode = Mode.WRITE)
    @Description("CALL algo.triangleCount.forkJoin(label, relationship, " +
            "{concurrency:4, write:true, writeProperty:'triangles', clusteringCoefficientProperty:'coefficient'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount, averageClusteringCoefficient")
    public Stream<Result> triangleCountExp3(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final Graph graph;
        final TriangleCountForkJoin triangleCount;
        final AtomicDoubleArray clusteringCoefficients;

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
                    .withSort(true)
                    .asUndirected(true)
                    .init(log, label, relationship, configuration)
                    .withDirection(TriangleCountBase.D)
                    .load(configuration.getGraphImpl(
                            HeavyGraph.TYPE,
                            HeavyGraph.TYPE, HeavyCypherGraphFactory.TYPE, HugeGraph.TYPE
                    ));
        }

        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        try (ProgressTimer timer = builder.timeEval()) {
            triangleCount = new TriangleCountForkJoin(
                    graph,
                    ForkJoinPool.commonPool(),
                    configuration.getNumber("threshold", 10_000).intValue())
                    .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                    .withTerminationFlag(terminationFlag)
                    .compute();
            clusteringCoefficients = triangleCount.getClusteringCoefficients();
        }

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                final Optional<String> coefficientProperty = configuration.getString(COEFFICIENT_WRITE_PROPERTY_VALUE);
                final Exporter exporter = Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build();
                if (coefficientProperty.isPresent()) {
                    exporter.write(
                            configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE),
                            triangleCount.getTriangles(),
                            Translators.ATOMIC_INTEGER_ARRAY_TRANSLATOR,
                            coefficientProperty.get(),
                            clusteringCoefficients,
                            Translators.ATOMIC_DOUBLE_ARRAY_TRANSLATOR
                    );
                } else {
                    exporter.write(
                            configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE),
                            triangleCount.getTriangles(),
                            Translators.ATOMIC_INTEGER_ARRAY_TRANSLATOR
                    );
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

        public Result(
                long loadMillis,
                long computeMillis,
                long writeMillis,
                long nodeCount,
                long triangleCount,
                double averageClusteringCoefficient) {
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
            return new Result(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount,
                    triangleCount,
                    averageClusteringCoefficient);
        }
    }

}
