package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;

class CategoricalInput implements  Comparable<CategoricalInput> {
    long id;
    long[] targets;

    public CategoricalInput(long id, long[] targets) {
        this.id = id;
        this.targets = targets;
    }

    @Override
    public int compareTo(CategoricalInput o) {
        return Long.compare(id, o.id);
    }

    SimilarityResult jaccard(double similarityCutoff, CategoricalInput e2) {
        long intersection = Intersections.intersection3(targets, e2.targets);
        if (similarityCutoff >= 0d && intersection == 0) return null;
        int count1 = targets.length;
        int count2 = e2.targets.length;
        long denominator = count1 + count2 - intersection;
        double jaccard = denominator == 0 ? 0 : (double)intersection / denominator;
        if (jaccard < similarityCutoff) return null;
        return new SimilarityResult(id, e2.id, count1, count2, intersection, jaccard);
    }
}
