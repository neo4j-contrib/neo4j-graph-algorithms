package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.procedures.IntProcedure;

import java.util.Arrays;

/**
 * @author mknblch
 */
public class Communities {

    final IntObjectMap<IntSet> communities;
    final int[] communityIds;

    public Communities(int capacity) {
        communityIds = new int[capacity];
        communities = new IntObjectScatterMap<>();
        reset();
    }

    public synchronized void set(int nodeId, int community) {
        IntSet intSet = communities.get(community);
        if (null == intSet) {
            intSet = new IntHashSet();
            communities.put(community, intSet);
        }
        intSet.add(nodeId);
        communityIds[nodeId] = community;
    }

    public int getCommunity(int node) {
        return communityIds[node];
    }

    public void forEachIn(int community, IntProcedure procedure) {
        final IntSet intCursors = communities.get(community);
        if (null == intCursors) {
            return;
        }
        intCursors.forEach(procedure);
    }

    public void reset() {
        Arrays.setAll(communityIds, i -> i);
        communities.clear();
    }
}
