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
package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.core.utils.AbstractExporter;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

public interface PageRankResult {

    long size();

    double score(int nodeId);

    double score(long nodeId);

    AbstractExporter<PageRankResult> exporter(
            GraphDatabaseAPI db,
            TerminationFlag terminationFlag,
            Log log,
            String writeProperty,
            ExecutorService executorService,
            int concurrency);

    default boolean hasFastToDoubleArray() {
        return false;
    }

    default double[] toDoubleArray() {
        int size = Math.toIntExact(size());
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = score(i);
        }
        return result;
    }
}
