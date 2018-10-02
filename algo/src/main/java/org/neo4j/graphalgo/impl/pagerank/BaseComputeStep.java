package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public abstract class BaseComputeStep implements ComputeStep {
    private static final int S_INIT = 0;
    private static final int S_CALC = 1;
    private static final int S_SYNC = 2;

    private int state;

    int[] starts;
    private int[] lengths;
    private int[] sourceNodeIds;
    final RelationshipIterator relationshipIterator;
    final Degrees degrees;

    private final double alpha;
    private final double dampingFactor;

    private double[] pageRank;
    double[] deltas;
    int[][] nextScores;
    private int[][] prevScores;

    private final int partitionSize;
    final int startNode;
    final int endNode;

    BaseComputeStep(
            double dampingFactor,
            int[] sourceNodeIds,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            int partitionSize,
            int startNode) {
        this.dampingFactor = dampingFactor;
        this.alpha = 1.0 - dampingFactor;
        this.sourceNodeIds = sourceNodeIds;
        this.relationshipIterator = relationshipIterator;
        this.degrees = degrees;
        this.partitionSize = partitionSize;
        this.startNode = startNode;
        this.endNode = startNode + partitionSize;
        state = S_INIT;
    }

    public void setStarts(int starts[], int[] lengths) {
        this.starts = starts;
        this.lengths = lengths;
    }

    @Override
    public void run() {
        if (state == S_CALC) {
            singleIteration();
            state = S_SYNC;
        } else if (state == S_SYNC) {
            synchronizeScores(combineScores());
            state = S_CALC;
        } else if (state == S_INIT) {
            initialize();
            state = S_CALC;
        }
    }

    private void initialize() {
        this.nextScores = new int[starts.length][];
        Arrays.setAll(nextScores, i -> new int[lengths[i]]);

        double[] partitionRank = new double[partitionSize];

        if(sourceNodeIds.length == 0) {
            Arrays.fill(partitionRank, alpha);
        } else {
            Arrays.fill(partitionRank,0);

            int[] partitionSourceNodeIds = IntStream.of(sourceNodeIds)
                    .filter(sourceNodeId -> sourceNodeId >= startNode && sourceNodeId < endNode)
                    .toArray();

            for (int sourceNodeId : partitionSourceNodeIds) {
                partitionRank[sourceNodeId - this.startNode] = alpha;
            }
        }


        this.pageRank = partitionRank;
        this.deltas = Arrays.copyOf(partitionRank, partitionSize);
    }

    abstract void singleIteration();

    public void prepareNextIteration(int[][] prevScores) {
        this.prevScores = prevScores;
    }

    private int[] combineScores() {
        assert prevScores != null;
        assert prevScores.length >= 1;
        int[][] prevScores = this.prevScores;

        int length = prevScores.length;
        int[] allScores = prevScores[0];
        for (int i = 1; i < length; i++) {
            int[] scores = prevScores[i];
            for (int j = 0; j < scores.length; j++) {
                allScores[j] += scores[j];
                scores[j] = 0;
            }
        }

        return allScores;
    }

    private void synchronizeScores(int[] allScores) {
        double dampingFactor = this.dampingFactor;
        double[] pageRank = this.pageRank;

        int length = allScores.length;
        for (int i = 0; i < length; i++) {
            int sum = allScores[i];

            double delta = dampingFactor * (sum / 100_000.0);
            pageRank[i] += delta;
            deltas[i] = delta;
            allScores[i] = 0;
        }
    }

    @Override
    public int[][] nextScores() {
        return nextScores;
    }

    @Override
    public double[] pageRank() {
        return pageRank;
    }

    @Override
    public int[] starts() {
        return starts;
    }

}
