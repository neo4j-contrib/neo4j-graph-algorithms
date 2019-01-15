package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;

public class LouvainUtils {

    /**
     * normalize nodeToCommunity-Array. Maps community IDs
     * in a sequential order starting at 0.
     *
     * @param communities
     * @return number of communities
     */
    static int normalize(int[] communities) {
        final IntIntMap map = new IntIntScatterMap(communities.length);
        int c = 0;
        for (int i = 0; i < communities.length; i++) {
            int mapped, community = communities[i];
            if ((mapped = map.getOrDefault(community, -1)) != -1) {
                communities[i] = mapped;
            } else {
                map.put(community, c);
                communities[i] = c++;
            }
        }
        return c;
    }
}
