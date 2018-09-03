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
package org.neo4j.graphalgo;

import java.util.Objects;

public class SimilarityResult {
    public final long source2;
    public final long count1;
    public final long source1;
    public final long count2;
    public final long intersection;
    public final double similarity;

    public static SimilarityResult TOMB = new SimilarityResult(-1, -1, -1, -1, -1, -1);

    public SimilarityResult(long source1, long source2, long count1, long count2, long intersection, double similarity) {
        this.source1 = source1;
        this.source2 = source2;
        this.count1 = count1;
        this.count2 = count2;
        this.intersection = intersection;
        this.similarity = similarity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimilarityResult that = (SimilarityResult) o;
        return source1 == that.source1 &&
                source2 == that.source2 &&
                count1 == that.count1 &&
                count2 == that.count2 &&
                intersection == that.intersection &&
                Double.compare(that.similarity, similarity) == 0;
    }

    @Override
    public int hashCode() {

        return Objects.hash(source1, source2, count1, count2, intersection, similarity);
    }

    public static SimilarityResult of(long source1, long source2, long[] targets1, long[] targets2, double similarityCutoff) {
        long intersection = JaccardProc.intersection3(targets1,targets2);
        if (similarityCutoff >= 0d && intersection == 0) return null;
        int count1 = targets1.length;
        int count2 = targets2.length;
        long denominator = count1 + count2 - intersection;
        double jaccard = denominator == 0 ? 0 : (double)intersection / denominator;
        if (jaccard < similarityCutoff) return null;
        return new SimilarityResult(source1, source2, count1, count2, intersection, jaccard);
    }
}
