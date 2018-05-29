/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.graphdb.Direction;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DeepGL extends Algorithm<DeepGL> {

    // the graph
    private Graph graph;
    // AI counts up for every node until nodeCount is reached
    private volatile AtomicInteger nodeQueue = new AtomicInteger();
    // atomic double array which supports only atomic-add
    private AtomicDoubleArray centrality;
    // the node count
    private final int nodeCount;
    // global executor service
    private final ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;
    private Direction direction = Direction.OUTGOING;
    private double divisor = 1.0;
    private volatile double[][] embedding;
    private volatile double[][] prevEmbedding;

    /**
     * constructs a parallel centrality solver
     *
     * @param graph           the graph iface
     * @param executorService the executor service
     * @param concurrency     desired number of threads to spawn
     */
    public DeepGL(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.embedding = new double[nodeCount][];
        this.prevEmbedding = new double[nodeCount][];
    }

    public DeepGL withDirection(Direction direction) {
        this.direction = direction;
        this.divisor = direction == Direction.BOTH ? 2.0 : 1.0;
        return this;
    }

    /**
     * compute centrality
     *
     * @return itself for method chaining
     */
    public DeepGL compute() {
        nodeQueue.set(0);
        final ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            futures.add(executorService.submit(new BaseFeaturesTask()));
        }
        ParallelUtil.awaitTermination(futures);

        nodeQueue.set(0);
        final ArrayList<Future<?>> normaliseFutures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            normaliseFutures.add(executorService.submit(new NormaliseTask(getGlobalMax())));
        }
        ParallelUtil.awaitTermination(normaliseFutures);

        prevEmbedding = embedding;
        embedding = new double[nodeCount][];

        nodeQueue.set(0);
        final ArrayList<Future<?>> featureFutures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            featureFutures.add(executorService.submit(new FeatureTask()));
        }
        ParallelUtil.awaitTermination(featureFutures);

        final int numFeatures = 6;
        double[] featureMaxes = calculateMax(numFeatures);
        System.out.println("featureMaxes = " + Arrays.toString(featureMaxes));

        nodeQueue.set(0);
        final ArrayList<Future<?>> moreNormaliseFutures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            moreNormaliseFutures.add(executorService.submit(new NormaliseTask(featureMaxes)));
        }
        ParallelUtil.awaitTermination(moreNormaliseFutures);

        return this;
    }

    private double[] calculateMax(int numFeatures) {
        double[] maxes = new double[numFeatures];

        int length = embedding[0].length / numFeatures;
        System.out.println("length = " + length);
        for (int columnIndex = 0; columnIndex < embedding[0].length; columnIndex++) {
            int maxPosition = columnIndex / length;

            System.out.println("maxPosition = " + maxPosition);
            for (double[] anEmbedding : embedding) {
                maxes[maxPosition] = maxes[maxPosition] < anEmbedding[columnIndex] ? anEmbedding[columnIndex] : maxes[maxPosition];
            }
        }

        return maxes;
    }

    private double getGlobalMax() {
        return Arrays.stream(embedding).parallel()
                    .mapToDouble(embedding -> embedding[2])
                    .max()
                    .getAsDouble();
    }

    /**
     * get the centrality array
     *
     * @return array with centrality
     */
    public AtomicDoubleArray getCentrality() {
        return centrality;
    }

    /**
     * emit the result stream
     *
     * @return stream if Results
     */
    public Stream<DeepGL.Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId ->
                        new DeepGL.Result(
                                graph.toOriginalNodeId(nodeId),
                                embedding[nodeId]));
    }

    @Override
    public DeepGL me() {
        return this;
    }

    @Override
    public DeepGL release() {
        graph = null;
        centrality = null;
        return null;
    }

    /**
     * a BaseFeaturesTask takes one element from the nodeQueue as long as
     * it is lower then nodeCount and calculates it's centrality
     */
    private class BaseFeaturesTask implements Runnable {

        @Override
        public void run() {
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }


                embedding[nodeId] = new double[]{
                        graph.degree(nodeId, Direction.INCOMING),
                        graph.degree(nodeId, Direction.OUTGOING),
                        graph.degree(nodeId, Direction.BOTH)
                };
            }
        }
    }

    public class Result {
        public final long nodeId;
        public final double[] embedding;

        public Result(long nodeId, double[] embedding) {
            this.nodeId = nodeId;
            this.embedding = embedding;
        }
    }

    private class NormaliseTask implements Runnable {


        private final double[] globalMax;

        public NormaliseTask(double... globalMax) {

            this.globalMax = globalMax;
        }

        @Override
        public void run() {
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                int sizeOfFeature = embedding[nodeId].length / globalMax.length;

                for (int i = 0; i < embedding[nodeId].length; i++) {
                    int feat = i / sizeOfFeature;

                    embedding[nodeId][i] /= globalMax[feat];
                }
            }
        }
    }

    private class FeatureTask implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                int numNeighbourhoods = 3;
                int numFeatures = 2;

                // for f in functions:
                //  for n in neighbours:

                int lengthOfEachFeature = prevEmbedding[0].length;
                embedding[nodeId] = new double[lengthOfEachFeature * numNeighbourhoods * numFeatures];
                Arrays.fill(embedding[nodeId], 0);

                RelOperator sum = new RelOperator() {

                    @Override
                    public void apply(int offset, int lengthOfEachFeature, int targetNodeId) {
                        for (int i = 0; i < lengthOfEachFeature; i++) {
                            embedding[nodeId][i + offset] += prevEmbedding[targetNodeId][i];
                        }
                    }

                    @Override
                    public void initialise(int offset, int lengthOfEachFeature, Direction direction) {
                        if (graph.degree(nodeId, direction) > 0) {
                            Arrays.fill(embedding[nodeId], offset, lengthOfEachFeature + offset, defaultVal());
                        }
                    }

                    @Override
                    public double defaultVal() {
                        return 0;
                    }
                };

                RelOperator hadamard = new RelOperator() {
                    @Override
                    public void apply(int offset, int lengthOfEachFeature, int targetNodeId) {
                        for (int i = 0; i < lengthOfEachFeature; i++) {
                            embedding[nodeId][i + offset] *= prevEmbedding[targetNodeId][i];
                        }
                    }

                    @Override
                    public void initialise(int offset, int lengthOfEachFeature, Direction direction) {
                        if (graph.degree(nodeId, direction) > 0) {
                            System.out.println("offset: " + offset + "length: " + lengthOfEachFeature);
                            Arrays.fill(embedding[nodeId], offset, lengthOfEachFeature + offset, defaultVal());
                        }
                    }

                    @Override
                    public double defaultVal() {
                        return 1;
                    }
                };

                RelOperator[] operators = new RelOperator[]{sum, hadamard};

                for (int i = 0; i < operators.length; i++) {
                    RelOperator operator = operators[i];
                    int offset = i * lengthOfEachFeature * numNeighbourhoods;


                    operator.initialise(offset, lengthOfEachFeature, Direction.OUTGOING);
                    graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        operator.apply(offset, lengthOfEachFeature, targetNodeId);
                        return true;
                    });

                    operator.initialise(offset + 3, lengthOfEachFeature, Direction.INCOMING);
                    graph.forEachRelationship(nodeId, Direction.INCOMING, (sourceNodeId, targetNodeId, relationId) -> {
                        operator.apply(offset + 3, lengthOfEachFeature, targetNodeId);
                        return true;
                    });

                    operator.initialise(offset + 6, lengthOfEachFeature, Direction.BOTH);
                    graph.forEachRelationship(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                        operator.apply(offset + 6, lengthOfEachFeature, targetNodeId);
                        return true;
                    });
                }

            }
        }

        void sum(int offset, int lengthOfEachFeature, int targetNodeId, double defaultValue) {
            for (int i = 0; i < lengthOfEachFeature; i++) {
                embedding[targetNodeId][i + offset] += prevEmbedding[targetNodeId][i];
            }
        }

    }
    interface RelOperator {
        void apply(int offset, int lengthOfEachFeature, int targetNodeId);
        void initialise(int offset, int lengthOfEachFeature, Direction direction);
        double defaultVal();
    }
}
