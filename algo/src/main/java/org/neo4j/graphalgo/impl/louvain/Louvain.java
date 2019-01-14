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
package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.neo4jview.GraphView;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Louvain Clustering Algorithm.
 * <p>
 * The algo performs modularity optimization as long as the
 * modularity keeps incrementing. Every optimization step leads
 * to an array of length nodeCount containing the nodeId->community mapping.
 * <p>
 * After each step a new graph gets built from the actual community mapping
 * and is used as input for the next step.
 *
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {

    private final int rootNodeCount;
    private int level;
    private final ExecutorService pool;
    private final int concurrency;
    private final AllocationTracker tracker;
    private ProgressLogger progressLogger;
    private TerminationFlag terminationFlag;
    private int[] communities;
    private int[][] dendrogram;
    private double[] nodeWeights;
    private Graph root;
    private int communityCount = 0;

    public Louvain(Graph graph,
                   ExecutorService pool,
                   int concurrency,
                   AllocationTracker tracker) {
        this.root = graph;
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        rootNodeCount = Math.toIntExact(graph.nodeCount());
        communities = new int[rootNodeCount];
        nodeWeights = new double[rootNodeCount];
        tracker.add(4 * rootNodeCount);
        communityCount = rootNodeCount;
        Arrays.setAll(communities, i -> i);
    }

    public Louvain compute(int maxLevel, int maxIterations) {
        // temporary graph
        Graph graph = this.root;
        // result arrays
        dendrogram = new int[maxLevel][];
        int nodeCount = rootNodeCount;
        for (level = 0; level < maxLevel; level++) {
            // start modularity optimization
            final ModularityOptimization modularityOptimization =
                    new ModularityOptimization(graph,
                            nodeId -> nodeWeights[nodeId],
                            pool,
                            concurrency,
                            tracker)
                            .withProgressLogger(progressLogger)
                            .withTerminationFlag(terminationFlag)
                            .compute(maxIterations);
            // rebuild graph based on the community structure
            final int[] communityIds = modularityOptimization.getCommunityIds();
            communityCount = ModularityOptimization.normalize(communityIds);
            // release the old algo instance
            modularityOptimization.release();
            progressLogger.log(
                    "level: " + (level + 1) +
                            " communities: " + communityCount +
                            " q: " + modularityOptimization.getModularity());
            if (communityCount >= nodeCount) {
                break;
            }
            nodeCount = communityCount;
            dendrogram[level] = rebuildCommunityStructure(communityIds);
            graph = rebuildGraph(graph, communityIds, communityCount);
        }
        dendrogram = Arrays.copyOf(dendrogram, level);
        return this;
    }

    public Louvain compute(WeightMapping communityMap, int maxLevel, int maxIterations) {
        BitSet comCount = new BitSet();
        Arrays.setAll(communities, i -> {
            final int t = (int) communityMap.get(i, -1.0);
            final int c = t == -1 ? i : t;
            comCount.set(c);
            return c;
        });
        // temporary graph
        int nodeCount = comCount.cardinality();
        Graph graph = rebuildGraph(this.root, communities, nodeCount);
        // result arrays
        dendrogram = new int[maxLevel][];

        for (level = 0; level < maxLevel; level++) {
            // start modularity optimization
            final ModularityOptimization modularityOptimization =
                    new ModularityOptimization(graph,
                            nodeId -> nodeWeights[nodeId],
                            pool,
                            concurrency,
                            tracker)
                            .withProgressLogger(progressLogger)
                            .withTerminationFlag(terminationFlag)
                            .compute(maxIterations);
            // rebuild graph based on the community structure
            final int[] communityIds = modularityOptimization.getCommunityIds();
            communityCount = ModularityOptimization.normalize(communityIds);
            // release the old algo instance
            modularityOptimization.release();
            progressLogger.log(
                    "level: " + (level + 1) +
                            " communities: " + communityCount +
                            " q: " + modularityOptimization.getModularity());
            if (communityCount >= nodeCount) {
                break;
            }
            nodeCount = communityCount;
            dendrogram[level] = rebuildCommunityStructure(communityIds);
            graph = rebuildGraph(graph, communityIds, communityCount);
        }
        dendrogram = Arrays.copyOf(dendrogram, level);
        return this;
    }

    /**
     * create a virtual graph based on the community structure of the
     * previous louvain round
     *
     * @param graph        previous graph
     * @param communityIds community structure
     * @return a new graph built from a community structure
     */
    private Graph rebuildGraph(Graph graph, int[] communityIds, int communityCount) {
        // count and normalize community structure
        final int nodeCount = communityIds.length;
        // bag of nodeId->{nodeId, ..}
        final IntObjectMap<IntScatterSet> relationships = new IntObjectScatterMap<>(nodeCount);
        // accumulated weights
        final LongDoubleScatterMap relationshipWeights = new LongDoubleScatterMap(nodeCount);
        // for each node in the current graph
        for (int i = 0; i < nodeCount; i++) {
            // map node nodeId to community nodeId
            final int source = communityIds[i];
            // get transitions from current node
            graph.forEachOutgoing(i, (s, t, r) -> {
                // mapping
                final int target = communityIds[t];
                final double value = graph.weightOf(s, t);
                if (source == target) {
                    nodeWeights[source] += value;
                }
                // add IN and OUT relation
                computeIfAbsent(relationships, target).add(source);
                computeIfAbsent(relationships, source).add(target);
                relationshipWeights.addTo(RawValues.combineIntInt(source, target), value / 2); // TODO validate
                relationshipWeights.addTo(RawValues.combineIntInt(target, source), value / 2);
                return true;
            });
        }

        // create temporary graph
        return new LouvainGraph(communityCount, relationships, relationshipWeights);
    }

    private int[] rebuildCommunityStructure(int[] communityIds) {
        // rebuild community array
        assert rootNodeCount == communities.length;
        final int[] ints = new int[rootNodeCount];
        Arrays.setAll(ints, i -> {
            return communityIds[communities[i]];
        });
        communities = ints;
        return communities;
    }

    /**
     * nodeId to community mapping array
     *
     * @return
     */
    public int[] getCommunityIds() {
        return communities;
    }

    public int[] getCommunityIds(int level) {
        return dendrogram[level];
    }

    public int[][] getDendrogram() {
        return dendrogram;
    }

    /**
     * number of outer iterations
     *
     * @return
     */
    public int getLevel() {
        return level;
    }

    /**
     * number of distinct communities
     *
     * @return
     */
    public long getCommunityCount() {
        return communityCount;
    }

    /**
     * result stream
     *
     * @return
     */
    public Stream<Result> resultStream() {
        return IntStream.range(0, rootNodeCount)
                .mapToObj(i -> new Result(i, communities[i]));
    }

    public Stream<StreamingResult> dendrogramStream(boolean includeIntermediateCommunities) {
        return IntStream.range(0, rootNodeCount)
                .mapToObj(i -> {
                    List<Long> communitiesList = null;
                    if (includeIntermediateCommunities) {
                        communitiesList = new ArrayList<>(dendrogram.length);
                        for (int[] community : dendrogram) {
                            communitiesList.add((long) community[i]);
                        }
                    }

                    return new StreamingResult(root.toOriginalNodeId(i), communitiesList, communities[i]);
                });
    }

    @Override
    public Louvain me() {
        return this;
    }

    @Override
    public Louvain release() {
        tracker.add(4 * rootNodeCount);
        communities = null;
        return this;
    }

    @Override
    public Louvain withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }

    @Override
    public Louvain withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    private static IntScatterSet computeIfAbsent(IntObjectMap<IntScatterSet> relationships, int n) {
        final IntScatterSet intCursors = relationships.get(n);
        if (null == intCursors) {
            final IntScatterSet newList = new IntScatterSet();
            relationships.put(n, newList);
            return newList;
        }
        return intCursors;
    }

    /**
     * result object
     */
    public static final class Result {

        public final long nodeId;
        public final long community;

        public Result(long id, long community) {
            this.nodeId = id;
            this.community = community;
        }
    }

    public static final class StreamingResult {
        public final long nodeId;
        public final List<Long> communities;
        public final long community;

        public StreamingResult(long nodeId, List<Long> communities, long community) {

            this.nodeId = nodeId;
            this.communities = communities;
            this.community = community;
        }
    }
}
