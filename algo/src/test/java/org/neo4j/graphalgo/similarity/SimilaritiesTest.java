/*
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SimilaritiesTest {

    private final List<Number> input;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<List<Number>> data() {
        return Arrays.asList(
                Arrays.asList(1, 2, 3),
                Arrays.asList(1, 2, 3, 3),
                Arrays.asList(104, 101, 108, 108, 111)
        );
    }

    public SimilaritiesTest(List<Number> input) {
        this.input = input;
    }

    @Test
    public void jaccardIdenticalInput() {
        // given identical input

        // when
        Similarities s = new Similarities();
        double result = s.jaccardSimilarity(input, input);

        // then
        assertEquals(1.0, result, 0.01);
    }

    @Test
    public void cosineIdenticalInput() {
        // given identical input

        // when
        Similarities s = new Similarities();
        double result = s.cosineSimilarity(input, input);

        // then
        assertEquals(1.0, result, 0.01);
    }

    @Test
    public void pearsonIdenticalInput() {
        // given identical input

        // when
        Similarities s = new Similarities();
        double result = s.pearsonSimilarity(input, input, Collections.emptyMap());

        // then
        assertEquals(1.0, result, 0.01);
    }

    @Test
    public void euclideanIdenticalInput() {
        // given identical input

        // when
        Similarities s = new Similarities();
        double result = s.euclideanSimilarity(input, input);

        // then
        assertEquals(1.0, result, 0.01);
    }

    @Test
    public void overlapIdenticalInput() {
        // given identical input

        // when
        Similarities s = new Similarities();
        double result = s.overlapSimilarity(input, input);

        // then
        assertEquals(1.0, result, 0.01);
    }
}
