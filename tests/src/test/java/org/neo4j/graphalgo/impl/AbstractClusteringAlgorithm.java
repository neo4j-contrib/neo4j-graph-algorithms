package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import io.netty.util.collection.IntObjectMap;
import org.neo4j.graphalgo.api.Graph;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntUnaryOperator;

/**
 * @author mknblch
 */
public abstract class AbstractClusteringAlgorithm<ME extends AbstractClusteringAlgorithm<ME>> extends Algorithm<ME> {

    private static final IntArrayList EMPTY = new IntArrayList(0);

    protected Graph graph;
    protected ExecutorService pool;
    protected final int concurrency;

    private int[] communityOfNode;
    private IntObjectMap<IntArrayList> communityMembers;
    private double[] communityScore; // sum of scores of all nodes in community n

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    public AbstractClusteringAlgorithm(Graph graph, ExecutorService pool, int concurrency) {
        this.graph = graph;
        this.pool = pool;
        this.concurrency = concurrency;
    }

    public void init(IntUnaryOperator communityFun, IntToDoubleFunction communityScoreFun) {
        Arrays.setAll(communityOfNode, communityFun);
        Arrays.setAll(communityScore, communityScoreFun);
    }

    public double getCommunityScore(int node) {
        return communityScore[node];
    }

    public abstract double nodeScore(int node);

    public void move(int node, int targetCommunity) {
        writeLocked(() -> {
            final int c = communityOfNode[node];
            final double s = nodeScore(node);
            // remove from current community
            communityMembers.getOrDefault(c, EMPTY).removeLast(node);
            // remove its score from its community
            communityScore[c] -= s;
            // add to new community
            IntArrayList intCursors = communityMembers.get(node);
            if (null == intCursors) {
                intCursors = new IntArrayList();
                communityMembers.put(targetCommunity, intCursors);
                intCursors.add(node);
            }
            // add its score to its new community
            communityScore[targetCommunity] += s;
        });
    }

    public void forEachIn(int community, IntConsumer consumer) {
        final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        final IntArrayList clone = communityMembers.getOrDefault(community, EMPTY).clone();
        readLock.unlock();
        clone.forEach((Consumer<? super IntCursor>) node -> consumer.accept(node.value));
    }

    public void readLock(Runnable runnable) {
        final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        runnable.run();
        readLock.unlock();
    }

    public void writeLocked(Runnable runnable) {
        writeLock.lock();
        runnable.run();
        writeLock.unlock();
    }

    @Override
    public ME release() {
        graph = null;
        pool = null;
        return me();
    }
}
