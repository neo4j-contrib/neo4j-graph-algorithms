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
import org.nd4j.linalg.ops.transforms.Transforms;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
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
    // the graph
    private Graph graph;
    // AI counts up for every node until nodeCount is reached
    private volatile AtomicInteger nodeQueue = new AtomicInteger();

    // the node count
    private final int nodeCount;
    // global executor service
    private final ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;

    private int iterations;
    private double pruningLambda;

    private final INDArray diffusionMatrix;
    private final INDArray adjacencyMatrixOut;
    private final INDArray adjacencyMatrixIn;
    private final INDArray adjacencyMatrixBoth;

    private Pruning.Feature[][] features;
    private Pruning.Feature[][] prevFeatures;

    private INDArray ndEmbedding;
    private INDArray ndPrevEmbedding;
    private int diffusionIterations;


    /**
     * constructs a parallel centrality solver
     *  @param graph              the graph iface
     * @param executorService    the executor service
     * @param concurrency        desired number of threads to spawn
     * @param pruningLambda
     * @param diffusionIterations
     */
    public DeepGL(Graph graph, ExecutorService executorService, int concurrency, int iterations, double pruningLambda, int diffusionIterations) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.ndEmbedding = Nd4j.create(nodeCount, 3);
        this.numNeighbourhoods = 3;
        this.iterations = iterations;
        this.pruningLambda = pruningLambda;
        this.diffusionIterations = diffusionIterations;

        adjacencyMatrixBoth = Nd4j.create(nodeCount, nodeCount);
        adjacencyMatrixOut = Nd4j.create(nodeCount, nodeCount);
        adjacencyMatrixIn = Nd4j.create(nodeCount, nodeCount);
        PrimitiveIntIterator nodes = graph.nodeIterator();
        while (nodes.hasNext()) {
            int nodeId = nodes.next();

            graph.forEachRelationship(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                adjacencyMatrixBoth.putScalar(nodeId, targetNodeId, 1);
                return true;
            });
            graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                adjacencyMatrixOut.putScalar(nodeId, targetNodeId, 1);
                return true;
            });
            graph.forEachRelationship(nodeId, Direction.INCOMING, (sourceNodeId, targetNodeId, relationId) -> {
                adjacencyMatrixIn.putScalar(nodeId, targetNodeId, 1);
                return true;
            });

//            diffusionMatrix.putScalar(nodeId, nodeId, 1d / graph.degree(nodeId, Direction.BOTH));
        }

//        System.out.println("adjacencyMatrix = \n" + adjacencyMatrixBoth);
//        System.out.println("adjacencyMatrixIn = \n" + adjacencyMatrixIn);
//        System.out.println("adjacencyMatrixOut = \n" + adjacencyMatrixOut);
        this.diffusionMatrix = InvertMatrix.invert(Nd4j.diag(adjacencyMatrixBoth.sum(0)), false).mmul(adjacencyMatrixBoth);
    }

    public DeepGL withDirection(Direction direction) {
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

//        System.out.println("ndEmbedding = \n" + ndEmbedding);
        doBinning();

        // move base features to prevEmbedding layer
        ndPrevEmbedding = ndEmbedding;
        prevFeatures = features;

        for (int iteration = 0; iteration < iterations; iteration++) {
            getProgressLogger().logProgress((double) iteration / iterations);
            getProgressLogger().log("Current layer: " + iteration);

            // swap the layers
            features = new Pruning.Feature[numNeighbourhoods * operators.length * prevFeatures.length][];

            // layer 1 ndFeatures
//            System.out.println("ndPrevEmbedding = \n" + ndPrevEmbedding);

            // OUT
            List<INDArray> arrays = new LinkedList<>();
            for (int opId = 0; opId < operators.length; opId++) {
                arrays.add(operators[opId].ndOp(ndPrevEmbedding, adjacencyMatrixOut));
                arrays.add(operators[opId].ndOp(ndPrevEmbedding, adjacencyMatrixIn));
                arrays.add(operators[opId].ndOp(ndPrevEmbedding, adjacencyMatrixBoth));

                int offset = opId * prevFeatures.length * numNeighbourhoods;

                Pruning.Feature[] outNeighbourhoodFeature = new Pruning.Feature[]{Pruning.Feature.values()[numNeighbourhoods * opId]};
                for (int j = 0; j < prevFeatures.length; j++) {
                    features[offset + j] = ArrayUtils.addAll(prevFeatures[j], outNeighbourhoodFeature);
                }

                Pruning.Feature[] inNeighbourhoodFeature = new Pruning.Feature[]{Pruning.Feature.values()[numNeighbourhoods * opId + 1]};
                for (int j = 0; j < prevFeatures.length; j++) {
                    features[offset + j + prevFeatures.length] = ArrayUtils.addAll(prevFeatures[j], inNeighbourhoodFeature);
                }

                Pruning.Feature[] bothNeighbourhoodFeature = new Pruning.Feature[]{Pruning.Feature.values()[numNeighbourhoods * opId + 2]};
                for (int j = 0; j < prevFeatures.length; j++) {
                    features[offset + j + (2 * prevFeatures.length)] = ArrayUtils.addAll(prevFeatures[j], bothNeighbourhoodFeature);
                }
            }

            ndEmbedding = Nd4j.hstack(arrays);

//            System.out.println("nd embedding = \n" + ndEmbedding);

            INDArray ndDiffused = Nd4j.create(ndEmbedding.shape());
            Nd4j.copy(ndEmbedding, ndDiffused);


            features = ArrayUtils.addAll(features, features);
            for (int i = features.length / 2; i < features.length; i++) {
                features[i] = ArrayUtils.addAll(features[i], Pruning.Feature.DIFFUSE);
            }

            for (int diffIteration = 0; diffIteration < diffusionIterations; diffIteration++) {
                ndDiffused = diffusionMatrix.mmul(ndDiffused);
            }

            ndEmbedding = Nd4j.concat(1, ndEmbedding, ndDiffused);

            doBinning();
            doPruning();

            HashSet<Pruning.Feature[]> uniqueFeaturesSet = new HashSet<>(Arrays.asList(this.features));
            HashSet<Pruning.Feature[]> prevFeaturesSet = new HashSet<>(Arrays.asList(this.prevFeatures));

            uniqueFeaturesSet.removeAll(prevFeaturesSet);
            if (uniqueFeaturesSet.size() == 0) {
                ndEmbedding = ndPrevEmbedding;
                features = prevFeatures;
                break;
            }

            // this layer contains concat of new learned features and prev layer
            // so set prev layer = this layer
            ndPrevEmbedding = ndEmbedding;
            prevFeatures = this.features;
        }

//        ndEmbedding = ndPrevEmbedding;
//        features = prevFeatures;

        return this;
    }

    private void doBinning() {
        new Binning().logBins(ndEmbedding);
    }

    private void doPruning() {
        int ndSizeBefore = ndEmbedding.size(1);

        Pruning pruning = new Pruning(pruningLambda);
        Pruning.Embedding prunedEmbedding = pruning.prune(new Pruning.Embedding(prevFeatures, ndPrevEmbedding), new Pruning.Embedding(features, ndEmbedding));

        features = prunedEmbedding.getFeatures();

        ndEmbedding = prunedEmbedding.getNDEmbedding();

        int ndSizeAfter = ndEmbedding.size(1);

        getProgressLogger().log("ND Pruning: Before: [" + ndSizeBefore + "], After: [" + ndSizeAfter + "]");
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

        public Result(long nodeId, INDArray ndEmbedding) {
            this.nodeId = nodeId;

            double[] row = new double[ndEmbedding.size(1)];
            for (int columnIndex = 0; columnIndex < ndEmbedding.size(1); columnIndex++) {
                row[columnIndex] = ndEmbedding.getDouble(columnIndex);
            }
            this.embedding = Arrays.asList(ArrayUtils.toObject(row));
        }
    }

    interface RelOperator {
        INDArray ndOp(INDArray features, INDArray adjacencyMatrix);
        double defaultVal();
    }

    RelOperator sum = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            return adjacencyMatrix.mmul(features);
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator hadamard = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            INDArray[] had = new INDArray[adjacencyMatrix.columns()];
            for (int column = 0; column < adjacencyMatrix.columns(); column++) {
                int finalColumn = column;
                int[] indexes = IntStream.range(0, adjacencyMatrix.rows())
                        .filter(r -> adjacencyMatrix.getDouble(finalColumn, r) != 0)
                        .toArray();

                if (indexes.length > 0) {
                    had[column] = Nd4j.ones(features.columns());
                    for (int index : indexes) {
                        had[column].muli(features.getRow(index));
                    }
                } else {
                    INDArray zeros = Nd4j.zeros(features.columns());
                    had[column] = zeros;
                }
            }
            return Nd4j.vstack(had);
        }

        @Override
        public double defaultVal() {
            return 1;
        }
    };

    RelOperator max = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            INDArray[] maxes = new INDArray[features.columns()];
            for (int fCol = 0; fCol < features.columns(); fCol++) {
                INDArray repeat = features.getColumn(fCol).repeat(1, adjacencyMatrix.columns());
                INDArray mul = adjacencyMatrix.transpose().mul(repeat);
                maxes[fCol] = mul.max(0).transpose();

            }
            return Nd4j.hstack(maxes);
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator mean = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            INDArray div = adjacencyMatrix.mmul(features).div(adjacencyMatrix.sum(1).repeat(1, features.columns()));
            // clear NaNs from div by 0 - these entries should have a 0 instead.
            Nd4j.clearNans(div);
            return div;
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator rbf = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            double sigma = 16;
            INDArray[] sumsOfSquareDiffs = new INDArray[adjacencyMatrix.rows()];
            for (int node = 0; node < adjacencyMatrix.rows(); node++) {
                INDArray nodeFeatures = features.getRow(node);
                INDArray adjs = adjacencyMatrix.getColumn(node).repeat(1, features.columns());
                INDArray repeat = nodeFeatures.repeat(0, features.rows()).mul(adjs);
                INDArray sub = repeat.sub(features.mul(adjs));
                sumsOfSquareDiffs[node] = Transforms.pow(sub, 2).sum(0);
            }
            INDArray sumOfSquareDiffs = Nd4j.vstack(sumsOfSquareDiffs).mul(-(1d / Math.pow(sigma, 2)));
            return Transforms.exp(sumOfSquareDiffs);
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator l1Norm = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            INDArray[] norms = new INDArray[adjacencyMatrix.rows()];
            for (int node = 0; node < adjacencyMatrix.rows(); node++) {
                INDArray nodeFeatures = features.getRow(node);
                INDArray adjs = adjacencyMatrix.transpose().getColumn(node).repeat(1, features.columns());
                INDArray repeat = nodeFeatures.repeat(0, features.rows()).mul(adjs);
                INDArray sub = repeat.sub(features.mul(adjs));
                INDArray norm = sub.norm1(0);
                norms[node] = norm;
            }
            return Nd4j.vstack(norms);
        }

        @Override
        public double defaultVal() {
            return 0;
        }
    };

    RelOperator[] operators = new RelOperator[]{sum, hadamard, max, mean, rbf, l1Norm};
//    RelOperator[] operators = new RelOperator[]{sum};


}
