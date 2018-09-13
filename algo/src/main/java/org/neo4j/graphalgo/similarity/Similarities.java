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
package org.neo4j.graphalgo.similarity;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashSet;
import java.util.List;

public class Similarities {

    @UserFunction("algo.similarity.jaccard")
    @Description("algo.similarity.jaccard([vector1], [vector2]) " +
            "given two collection vectors, calculate jaccard similarity")
    public double jaccardSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1 == null || vector2 == null) return 0;

        HashSet<Number> intersectionSet = new HashSet<>(vector1);
        intersectionSet.retainAll(vector2);
        int intersection = intersectionSet.size();

        long denominator = vector1.size() + vector2.size() - intersection;
        return denominator == 0 ? 0 : (double)intersection / denominator;
    }

    @UserFunction("algo.similarity.cosine")
    @Description("algo.similarity.cosine([vector1], [vector2]) " +
            "given two collection vectors, calculate cosine similarity")
    public double cosineSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.size() == 0) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }

        double dotProduct = 0d;
        double xLength = 0d;
        double yLength = 0d;
        for (int i = 0; i < vector1.size(); i++) {
            double weight1 = vector1.get(i).doubleValue();
            double weight2 = vector2.get(i).doubleValue();

            dotProduct += weight1 * weight2;
            xLength += weight1 * weight1;
            yLength += weight2 * weight2;
        }

        xLength = Math.sqrt(xLength);
        yLength = Math.sqrt(yLength);

        return dotProduct / (xLength * yLength);
    }

    @UserFunction("algo.similarity.euclideanDistance")
    @Description("algo.similarity.euclideanDistance([vector1], [vector2]) " +
            "given two collection vectors, calculate the euclidean distance (square root of the sum of the squared differences)")
    public double euclideanDistance(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.size() == 0) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }

        double distance = 0.0;
        for (int i = 0; i < vector1.size(); i++) {
            double sqOfDiff = vector1.get(i).doubleValue() - vector2.get(i).doubleValue();
            sqOfDiff *= sqOfDiff;
            distance += sqOfDiff;
        }
        distance = Math.sqrt(distance);

        return distance;
    }

    @UserFunction("algo.similarity.euclidean")
    @Description("algo.similarity.euclidean([vector1], [vector2]) " +
            "given two collection vectors, calculate similarity based on euclidean distance")
    public double euclideanSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        return 1.0d / (1 + euclideanDistance(vector1, vector2));
    }
}