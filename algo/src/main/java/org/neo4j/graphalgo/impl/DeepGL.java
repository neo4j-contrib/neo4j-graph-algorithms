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

import org.apache.commons.lang3.ArrayUtils;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.inverse.InvertMatrix;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DeepGL extends Algorithm<DeepGL> {

    private final int numNeighbourhoods;
    private final INDArray diffusionMatrix;
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
    private INDArray ndEmbedding;
    private INDArray ndPrevEmbedding;
    private volatile int[] seenSoFarMapping;
    private volatile double[][] diffusion;
    private volatile double[][] prevDiffusion;
    private volatile Pruning.Feature[][] features;
    private volatile Pruning.Feature[][] prevFeatures;
    private int iterations;
    private double pruningLambda;
    private boolean applyNormalisation;

    /**
     * constructs a parallel centrality solver
     *
     * @param graph              the graph iface
     * @param executorService    the executor service
     * @param concurrency        desired number of threads to spawn
     * @param pruningLambda
     * @param applyNormalisation
     */
    public DeepGL(Graph graph, ExecutorService executorService, int concurrency, int iterations, double pruningLambda, boolean applyNormalisation) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.applyNormalisation = applyNormalisation;
        this.embedding = new double[nodeCount][];
        this.ndEmbedding = Nd4j.create(nodeCount, 3);
        this.prevEmbedding = new double[nodeCount][];
        this.numNeighbourhoods = 3;
        this.seenSoFarMapping = new int[nodeCount];
        this.diffusion = new double[nodeCount][];
        this.prevDiffusion = new double[nodeCount][];
        this.iterations = iterations;
        this.pruningLambda = pruningLambda;

        INDArray adjacencyMarix = Nd4j.create(nodeCount, nodeCount);
        PrimitiveIntIterator nodes = graph.nodeIterator();
        while (nodes.hasNext()) {
            int nodeId = nodes.next();

            graph.forEachRelationship(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                adjacencyMarix.putScalar(nodeId, targetNodeId, 1);
                return true;
            });

//            diffusionMatrix.putScalar(nodeId, nodeId, 1d / graph.degree(nodeId, Direction.BOTH));
        }

        this.diffusionMatrix = InvertMatrix.invert(Nd4j.diag(adjacencyMarix.sum(0)), false).mmul(adjacencyMarix);
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
        // base features
        nodeQueue.set(0);
        final ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            futures.add(executorService.submit(new BaseFeaturesTask()));
        }
        ParallelUtil.awaitTermination(futures);
        this.features = new Pruning.Feature[][]{
                {Pruning.Feature.IN_DEGREE},
                {Pruning.Feature.OUT_DEGREE},
                {Pruning.Feature.BOTH_DEGREE}
        };

        doBinning();

        // normalise
        if (applyNormalisation) {
            double featureMaxes = getGlobalMax();
            applyNormalisation(featureMaxes);
        }

        for (int iteration = 0; iteration < iterations; iteration++) {
            getProgressLogger().logProgress(iteration / iterations);
            getProgressLogger().log("Current layer: " + iteration);

            // swap the layers
            prevEmbedding = embedding;
            ndPrevEmbedding = ndEmbedding;

            embedding = new double[nodeCount][];
            ndEmbedding = Nd4j.create(nodeCount, numNeighbourhoods * operators.length * ndEmbedding.size(1));

            prevFeatures = features;
            features = new Pruning.Feature[numNeighbourhoods * operators.length * prevFeatures.length][];

            // layer 1 features
            nodeQueue.set(0);
            final ArrayList<Future<?>> featureFutures = new ArrayList<>();
            for (int i = 0; i < concurrency; i++) {
                featureFutures.add(executorService.submit(new FeatureTask()));
            }
            ParallelUtil.awaitTermination(featureFutures);

            // diffusion
            for (int i = 0; i < embedding.length; i++) {
                prevDiffusion[i] = new double[embedding[0].length / 2];
                System.arraycopy(embedding[i], 0, prevDiffusion[i], 0, embedding[i].length / 2);
            }

            INDArray ndDiffused = Nd4j.create(ndEmbedding.shape());
            Nd4j.copy(ndEmbedding, ndDiffused);


            features = ArrayUtils.addAll(features, features);
            for (int i = features.length / 2; i < features.length; i++) {
                features[i] = ArrayUtils.addAll(new Pruning.Feature[]{Pruning.Feature.DIFFUSE}, features[i]);
            }

            for (int diffIteration = 0; diffIteration < 10; diffIteration++) {

                diffusion = new double[nodeCount][];
                for (int j = 0; j < embedding.length; j++) {
                    diffusion[j] = new double[embedding[0].length / 2];
                }

                nodeQueue.set(0);
                final ArrayList<Future<?>> diffusionFutures = new ArrayList<>();
                for (int i = 0; i < concurrency; i++) {
                    diffusionFutures.add(executorService.submit(new DiffusionTask()));
                }
                ParallelUtil.awaitTermination(diffusionFutures);

//                for (int column = 0; column < ndEmbedding.size(1); column++) {
//                    ndDiffused.putColumn(column, diffusionMatrix.mmul(ndEmbedding.getColumn(column)));
//                }

                ndDiffused = diffusionMatrix.mmul(ndDiffused);

                prevDiffusion = diffusion;

            }

            for (int i = 0; i < embedding.length; i++) {
                System.arraycopy(diffusion[i], 0, embedding[i], embedding[i].length / 2, embedding[i].length / 2);
            }

            ndEmbedding = Nd4j.concat(1, ndEmbedding, ndDiffused);

            doBinning();

            if (applyNormalisation) {
                final int numFeatures = operators.length * numNeighbourhoods;
                double[] featureMaxes = calculateMax(numFeatures);
                applyNormalisation(featureMaxes);
            }

            doPruning();
        }

        return this;
    }

    private void applyNormalisation(double... featureMaxes) {
        nodeQueue.set(0);
        final ArrayList<Future<?>> normaliseFutures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            normaliseFutures.add(executorService.submit(new NormaliseTask(featureMaxes)));
        }
        ParallelUtil.awaitTermination(normaliseFutures);
    }

    private void doBinning() {
//        new Binning().linearBins(embedding, 100);
//        new Binning().logBins(embedding);
        new Binning().logBins(embedding);
        new Binning().logBins(ndEmbedding);
    }

    private void doPruning() {
        int sizeBefore = embedding[0].length;
        int ndSizeBefore = ndEmbedding.size(1);

        Pruning pruning = new Pruning(pruningLambda);
        Pruning.Embedding prunedEmbedding = pruning.prune(new Pruning.Embedding(prevFeatures, prevEmbedding, ndPrevEmbedding), new Pruning.Embedding(features, embedding, ndEmbedding));
        features = prunedEmbedding.getFeatures();
        embedding = prunedEmbedding.getEmbedding();

        ndEmbedding = prunedEmbedding.getNDEmbedding();

        int sizeAfter = embedding[0].length;
        int ndSizeAfter = ndEmbedding.size(1);

        getProgressLogger().log("Pruning: Before: [" + sizeBefore + "], After: [" + sizeAfter + "]");
        getProgressLogger().log("ND Pruning: Before: [" + ndSizeBefore + "], After: [" + ndSizeAfter + "]");
    }

    private double[] calculateMax(int numFeatures) {
        double[] maxes = new double[numFeatures];

        int length = embedding[0].length / numFeatures;
        for (int columnIndex = 0; columnIndex < embedding[0].length; columnIndex++) {
            int maxPosition = columnIndex / length;

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
                                embedding[nodeId],
                                ndEmbedding.getRow(nodeId)));
    }

    public Stream<Pruning.Feature[]> featureStream() {
        return Arrays.stream(features);
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

                ndEmbedding.putRow(nodeId, Nd4j.create(new double[]{
                        graph.degree(nodeId, Direction.INCOMING),
                        graph.degree(nodeId, Direction.OUTGOING),
                        graph.degree(nodeId, Direction.BOTH)
                }));
            }
        }
    }

    public class Result {
        public final long nodeId;
        public final List<Double> embedding;
        public final List<Double> ndEmbedding;

        public Result(long nodeId, double[] embedding, INDArray ndEmbedding) {
            this.nodeId = nodeId;
            this.embedding = Arrays.asList(ArrayUtils.toObject(embedding));

            double[] row = new double[ndEmbedding.size(1)];
            for (int columnIndex = 0; columnIndex < ndEmbedding.size(1); columnIndex++) {
                row[columnIndex] = ndEmbedding.getDouble(columnIndex);
            }
            this.ndEmbedding = Arrays.asList(ArrayUtils.toObject(row));
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

                int numFeatures = prevEmbedding[0].length;
                embedding[nodeId] = new double[numFeatures * numNeighbourhoods * operators.length * 2];
                Arrays.fill(embedding[nodeId], 0);


                for (int i = 0; i < operators.length; i++) {
                    RelOperator operator = operators[i];
                    int offset = i * numFeatures * numNeighbourhoods;

                    operator.apply(nodeId, offset, numFeatures, Direction.OUTGOING);
                    Pruning.Feature[] outNeighbourhoodFeature = new Pruning.Feature[]{Pruning.Feature.values()[numNeighbourhoods * i]};
                    for (int j = 0; j < prevFeatures.length; j++) {
                        features[offset + j] = ArrayUtils.addAll(outNeighbourhoodFeature, prevFeatures[j]);
                    }

                    operator.apply(nodeId, offset + numNeighbourhoods, numFeatures, Direction.INCOMING);
                    Pruning.Feature[] inNeighbourhoodFeature = new Pruning.Feature[]{Pruning.Feature.values()[numNeighbourhoods * i + 1]};
                    for (int j = 0; j < prevFeatures.length; j++) {
                        features[offset + j + prevFeatures.length] = ArrayUtils.addAll(inNeighbourhoodFeature, prevFeatures[j]);
                    }

                    operator.apply(nodeId, offset + 2 * numNeighbourhoods, numFeatures, Direction.BOTH);
                    Pruning.Feature[] bothNeighbourhoodFeature = new Pruning.Feature[]{Pruning.Feature.values()[numNeighbourhoods * i + 2]};
                    for (int j = 0; j < prevFeatures.length; j++) {
                        features[offset + j + 2 * prevFeatures.length] = ArrayUtils.addAll(bothNeighbourhoodFeature, prevFeatures[j]);
                    }
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
        void apply(int nodeId, int offset, int numOfFeatures, Direction direction);

        double defaultVal();
    }

    RelOperator sum = new RelOperator() {

        @Override
        public void apply(int nodeId, int offset, int numOfFeatures, Direction direction) {
            if (graph.degree(nodeId, direction) > 0) {
                Arrays.fill(embedding[nodeId], offset, numOfFeatures + offset, defaultVal());
            }

            graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId, relationId) -> {
                for (int i = 0; i < numOfFeatures; i++) {
                    embedding[nodeId][i + offset] += prevEmbedding[targetNodeId][i];
                    ndEmbedding.putScalar(nodeId, i + offset, ndEmbedding.getDouble(nodeId, i + offset) + (ndPrevEmbedding.getDouble(targetNodeId, i)));
                }
                return true;
            });
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator hadamard = new RelOperator() {
        @Override
        public void apply(int nodeId, int offset, int numOfFeatures, Direction direction) {
            if (graph.degree(nodeId, direction) > 0) {
                Arrays.fill(embedding[nodeId], offset, numOfFeatures + offset, defaultVal());
            }

            graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId, relationId) -> {
                for (int i = 0; i < numOfFeatures; i++) {
                    embedding[nodeId][i + offset] *= prevEmbedding[targetNodeId][i];
                }
                return true;
            });
        }

        @Override
        public double defaultVal() {
            return 1;
        }
    };

    RelOperator max = new RelOperator() {

        @Override
        public void apply(int nodeId, int offset, int numOfFeatures, Direction direction) {
            if (graph.degree(nodeId, direction) > 0) {
                Arrays.fill(embedding[nodeId], offset, numOfFeatures + offset, defaultVal());
            }

            graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId, relationId) -> {
                for (int i = 0; i < numOfFeatures; i++) {
                    if (prevEmbedding[targetNodeId][i] > embedding[nodeId][i + offset]) {
                        embedding[nodeId][i + offset] = prevEmbedding[targetNodeId][i];
                    }
                }
                return true;
            });

        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator mean = new RelOperator() {
        @Override
        public void apply(int nodeId, int offset, int numOfFeatures, Direction direction) {
            if (graph.degree(nodeId, direction) > 0) {
                Arrays.fill(embedding[nodeId], offset, numOfFeatures + offset, defaultVal());
            }
            int[] seenSoFar = {0};

            graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId, relationId) -> {
                for (int i = 0; i < numOfFeatures; i++) {
                    embedding[nodeId][i + offset] = (embedding[nodeId][i + offset] * seenSoFar[0] + prevEmbedding[targetNodeId][i]) / (seenSoFar[0] + 1);
                }
                seenSoFar[0]++;
                return true;
            });
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator rbf = new RelOperator() {
        @Override
        public void apply(int nodeId, int offset, int numOfFeatures, Direction direction) {
            if (graph.degree(nodeId, direction) > 0) {
                Arrays.fill(embedding[nodeId], offset, numOfFeatures + offset, defaultVal());
            }

            double[] sum = new double[numOfFeatures];
            graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId, relationId) -> {
                for (int i = 0; i < numOfFeatures; i++) {
                    sum[i] += Math.pow(prevEmbedding[targetNodeId][i] - prevEmbedding[nodeId][i], 2);
                }
                return true;
            });

            double sigma = 1;
            for (int i = 0; i < sum.length; i++) {
                embedding[nodeId][i + offset] = Math.exp((-1 / Math.pow(sigma, 2)) * sum[i]);
            }
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator l1Norm = new RelOperator() {
        @Override
        public void apply(int nodeId, int offset, int numOfFeatures, Direction direction) {
            if (graph.degree(nodeId, direction) > 0) {
                Arrays.fill(embedding[nodeId], offset, numOfFeatures + offset, defaultVal());
            }

            graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId, relationId) -> {
                for (int i = 0; i < numOfFeatures; i++) {
                    embedding[nodeId][i + offset] += Math.abs(prevEmbedding[targetNodeId][i] - prevEmbedding[nodeId][i]);
                }
                return true;
            });
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator[] operators = new RelOperator[]{sum, hadamard, max, mean, rbf, l1Norm};
//    RelOperator[] operators = new RelOperator[]{sum};


    private class DiffusionTask implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                graph.forEachRelationship(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                    int degreeOfTarget = graph.degree(targetNodeId, Direction.BOTH);

                    for (int i = 0; i < prevDiffusion[targetNodeId].length; i++) {
                        diffusion[nodeId][i] += prevDiffusion[targetNodeId][i] / degreeOfTarget;
                    }
                    return true;
                });
            }
        }
    }
}
