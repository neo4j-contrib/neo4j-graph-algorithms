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
package org.neo4j.graphalgo.algo.similarity;

import org.junit.Test;
import org.neo4j.graphalgo.similarity.Similarities;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PearsonSimilarityTest {

    @Test
    public void identicalVectors() {
        Similarities similarities = new Similarities();

        List<Number> user1Ratings = Arrays.asList(5, 6, 7, 8, 9);
        List<Number> user2Ratings = Arrays.asList(4, 5, 6, 7, 8);
        double similarity = similarities.pearsonSimilarity(user1Ratings, user2Ratings);
        assertEquals(1.0, similarity, 0.01);
    }

    @Test
    public void oppositeVectors() {
        Similarities similarities = new Similarities();


        List<Number> user1Ratings = Arrays.asList(9, 8, 7, 6, 5);
        List<Number> user2Ratings = Arrays.asList(4, 5, 6, 7, 8);
        double similarity = similarities.pearsonSimilarity(user1Ratings, user2Ratings);
        assertEquals(-1.0, similarity, 0.01);
    }

}
