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
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
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
    private HeavyGraph graph;
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

    private Pruning.Feature[] features;
    private Pruning.Feature[] prevFeatures;

    private INDArray ndEmbedding;
    private INDArray ndPrevEmbedding;
    private int diffusionIterations;

    private int numberOfLayers;

    /**
     * constructs a parallel centrality solver
     *
     * @param graph               the graph iface
     * @param executorService     the executor service
     * @param concurrency         desired number of threads to spawn
     * @param pruningLambda
     * @param diffusionIterations
     */
    public DeepGL(HeavyGraph graph, ExecutorService executorService, int concurrency, int iterations, double pruningLambda, int diffusionIterations) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.ndEmbedding = Nd4j.create(nodeCount, 3 + graph.availableNodeProperties().size());
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
        }

        this.diffusionMatrix = InvertMatrix.invert(Nd4j.diag(adjacencyMatrixBoth.sum(0)), false).mmul(adjacencyMatrixBoth);
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

        Set<String> nodeProperties = graph.availableNodeProperties();
        this.features = new Pruning.Feature[3 + nodeProperties.size()];
        this.features[0] = new Pruning.Feature("IN_DEGREE");
        this.features[1] = new Pruning.Feature("OUT_DEGREE");
        this.features[2] = new Pruning.Feature("BOTH_DEGREE");

        Iterator<String> iterator = nodeProperties.iterator();
        int counter = 3;

        while (iterator.hasNext()) {
            this.features[counter] = new Pruning.Feature(iterator.next().toUpperCase());
            counter++;
        }


        doBinning();

        ndPrevEmbedding = ndEmbedding;
        prevFeatures = features;

        int iteration;
        for (iteration = 1; iteration <= iterations; iteration++) {
            getProgressLogger().logProgress((double) iteration / iterations);
            getProgressLogger().log("Current layer: " + iteration);

            features = new Pruning.Feature[numNeighbourhoods * operators.length * prevFeatures.length];

            List<INDArray> arrays = new LinkedList<>();
            List<Pruning.Feature> featuresList = new LinkedList<>();
            for (RelOperator operator : operators) {
                arrays.add(operator.ndOp(ndPrevEmbedding, adjacencyMatrixOut));
                arrays.add(operator.ndOp(ndPrevEmbedding, adjacencyMatrixIn));
                arrays.add(operator.ndOp(ndPrevEmbedding, adjacencyMatrixBoth));

                for (String neighbourhood : new String[]{"_out", "_in", "_both"}) {
                    for (Pruning.Feature prevFeature : prevFeatures) {
                        featuresList.add(new Pruning.Feature(operator.name() + neighbourhood + "_neighbourhood", prevFeature));
                    }
                }
            }

            ndEmbedding = Nd4j.hstack(arrays);

            INDArray ndDiffused = Nd4j.create(ndEmbedding.shape());
            Nd4j.copy(ndEmbedding, ndDiffused);

            featuresList.addAll(featuresList);
            features = featuresList.toArray(new Pruning.Feature[0]);

            for (int i = features.length / 2; i < features.length; i++) {
                features[i] = new Pruning.Feature("diffuse", features[i]);
            }

            for (int diffIteration = 0; diffIteration < diffusionIterations; diffIteration++) {
                ndDiffused = diffusionMatrix.mmul(ndDiffused);
            }

            ndEmbedding = Nd4j.concat(1, ndEmbedding, ndDiffused);

            doBinning();
            doPruning();

            HashSet<Pruning.Feature> uniqueFeaturesSet = new HashSet<>(Arrays.asList(this.features));
            HashSet<Pruning.Feature> prevFeaturesSet = new HashSet<>(Arrays.asList(this.prevFeatures));

            uniqueFeaturesSet.removeAll(prevFeaturesSet);
            if (uniqueFeaturesSet.size() == 0) {
                ndEmbedding = ndPrevEmbedding;
                features = prevFeatures;
                this.numberOfLayers = iteration;
                break;
            }

            ndPrevEmbedding = ndEmbedding;
            prevFeatures = this.features;
        }

        this.numberOfLayers = iteration;

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

    public INDArray embedding() {
        return ndEmbedding;
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

    public Stream<Pruning.Feature> featureStream() {
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

    public int numberOfLayers() {
        return numberOfLayers;
    }

    public Pruning.Feature[] features() {
        return features;
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

                Set<String> nodeProperties = graph.availableNodeProperties();

                double[] row = new double[3 + nodeProperties.size()];
                row[0] = graph.degree(nodeId, Direction.INCOMING);
                row[1] = graph.degree(nodeId, Direction.OUTGOING);
                row[2] = graph.degree(nodeId, Direction.BOTH);

                Iterator<String> iterator = nodeProperties.iterator();
                int counter = 3;

                while (iterator.hasNext()) {
                    row[counter] = graph.nodeProperties(iterator.next()).get(nodeId);
                    counter++;
                }

                ndEmbedding.putRow(nodeId, Nd4j.create(row));
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

        String name();
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

        @Override
        public String name() {
            return "sum";
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

        @Override
        public String name() {
            return "hadamard";
        }
    };

    RelOperator max = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            INDArray[] maxes = new INDArray[features.columns()];
            for (int fCol = 0; fCol < features.columns(); fCol++) {
                INDArray mul = adjacencyMatrix.transpose().mulColumnVector(features.getColumn(fCol));
                maxes[fCol] = mul.max(0).transpose();
            }
            return Nd4j.hstack(maxes);
        }

        @Override
        public double defaultVal() {
            return 0;
        }

        @Override
        public String name() {
            return "max";
        }
    };

    RelOperator mean = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            INDArray mean = adjacencyMatrix
                    .mmul(features)
                    .diviColumnVector(adjacencyMatrix.sum(1));
            // clear NaNs from div by 0 - these entries should have a 0 instead.
            Nd4j.clearNans(mean);
            return mean;
        }

        @Override
        public double defaultVal() {
            return 0;
        }

        @Override
        public String name() {
            return "mean";
        }
    };

    RelOperator rbf = new RelOperator() {

        @Override
        public INDArray ndOp(INDArray features, INDArray adjacencyMatrix) {
            double sigma = 16;
            INDArray[] sumsOfSquareDiffs = new INDArray[adjacencyMatrix.rows()];
            for (int node = 0; node < adjacencyMatrix.rows(); node++) {
                INDArray column = adjacencyMatrix.getColumn(node);
                INDArray repeat = features.getRow(node).repeat(0, features.rows()).muliColumnVector(column);
                INDArray sub = repeat.sub(features.mulColumnVector(column));
                sumsOfSquareDiffs[node] = Transforms.pow(sub, 2).sum(0);
            }
            INDArray sumOfSquareDiffs = Nd4j.vstack(sumsOfSquareDiffs).muli(-(1d / Math.pow(sigma, 2)));
            return Transforms.exp(sumOfSquareDiffs);
        }

        @Override
        public double defaultVal() {
            return 0;
        }

        @Override
        public String name() {
            return "rbf";
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

        @Override
        public String name() {
            return "l1Norm";
        }
    };

    RelOperator[] operators = new RelOperator[]{sum, hadamard, max, mean, rbf, l1Norm};
//    RelOperator[] operators = new RelOperator[]{sum};


}
