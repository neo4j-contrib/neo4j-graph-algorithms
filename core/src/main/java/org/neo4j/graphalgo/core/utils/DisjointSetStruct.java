package org.neo4j.graphalgo.core.utils;

public class DisjointSetStruct {

    // TODO add ranking / quick union

    private int[] set;
    private int offset;

    public DisjointSetStruct(int capacity) {
        offset = 0;
        set = new int[capacity];
        for (int i = 0; i < capacity; i++) {
            set[i] = i;
        }
    }

    public int find(int p) {
        while (p != set[p]) {
            p = set[set[p]];
        }
        return p;
    }

    public int size() {
        return offset;
    }
  
    public boolean isConnected(int p, int q) {
        return find(p) == find(q);
    }
  
    public void union(int p, int q) {
        int pSet = find(p);
        int qSet = find(q);
        if (pSet == qSet) {
            return;
        }
        set[pSet] = qSet;
    }

    public int[] getSet() {
        return set;
    }
}