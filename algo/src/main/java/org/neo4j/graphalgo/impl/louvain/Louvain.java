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


import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> implements LouvainAlgorithm {

    private RelationshipIterator relationshipIterator;
    private final Degrees degrees;
    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;

    private final int[] communityIds; // node to community mapping
    private final double[] sTot;
    private final IdMapping idMapping;
    private int iterations;
    private double m2, mq2;
    private final int maxIterations;

    public Louvain(IdMapping idMapping,
                   RelationshipIterator relationshipIterator,
                   Degrees degrees,
                   ExecutorService executorService,
                   int concurrency, int maxIterations) {

        this.idMapping = idMapping;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
        this.relationshipIterator = relationshipIterator;
        this.degrees = degrees;
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        communityIds = new int[nodeCount];
        sTot = new double[nodeCount];

    }

    public LouvainAlgorithm compute() {
        reset();
        for (this.iterations = 0; this.iterations < maxIterations; this.iterations++) {
            if (!arrange()) {
                return this;
            }
        }
        return this;
    }


    @Override
    public Louvain release() {
        relationshipIterator = null;
        executorService = null;
        return this;
    }

    private void reset() {

        Arrays.setAll(communityIds, i -> i);

        final LongAdder adder = new LongAdder();
        ParallelUtil.iterateParallel(executorService, nodeCount, concurrency, node -> {
            final int d = degrees.degree(node, Direction.BOTH);
            sTot[node] = d;
            adder.add(d);
        });
        m2 = adder.intValue() * 4.0; // 2m //
        mq2 = 2.0 * Math.pow(adder.intValue(), 2.0); // 2m^2
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param targetCommunity communityId
     */
    private void assign(int node, int targetCommunity) {
        final int d = degrees.degree(node, Direction.BOTH);
        sTot[communityIds[node]] -= d;
        sTot[targetCommunity] += d;
        // update communityIds
        communityIds[node] = targetCommunity;
    }

     /**
     * @return kiIn
     */
    private int kIIn(int node, int targetCommunity) {
        int[] sum = {0}; // {ki, ki_in}
        relationshipIterator.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
            if (targetCommunity == communityIds[targetNodeId]) {
                sum[0]++;
            }
            return true;
        });

        return sum[0];
    }

    /**
     * implements phase 1 of louvain
     * @return
     */
    private boolean arrange() {
        final boolean[] changes = {false};
        final double[] bestGain = {0};
        final int[] bestCommunity = {0};
        for (int node = 0; node < nodeCount; node++) {
            bestGain[0] = 0.0;
            final int sourceCommunity = bestCommunity[0] = communityIds[node];
            final double mSource = (sTot[sourceCommunity] * degrees.degree(node, Direction.BOTH)) / mq2;
            relationshipIterator.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                final int targetCommunity = communityIds[targetNodeId];
                final double gain = kIIn(sourceNodeId, targetCommunity) / m2 - mSource;
                if (gain > bestGain[0]) {
                    bestCommunity[0] = targetCommunity;
                    bestGain[0] = gain;
                }
                return true;
            });
            if (bestCommunity[0] != sourceCommunity) {
                assign(node, bestCommunity[0]);
                changes[0] = true;
            }
        }
        return changes[0];
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i ->
                        new Result(idMapping.toOriginalNodeId(i), communityIds[i]));
    }

    public int[] getCommunityIds() {
        return communityIds;
    }

    public int getIterations() {
        return iterations;
    }

    public int getCommunityCount() {
        final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
        for (int i = 0; i < communityIds.length; i++) {
            bitSet.put(communityIds[i]);
        }
        return bitSet.size();
    }

    @Override
    public Louvain me() {
        return this;
    }

}
