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
package org.neo4j.graphalgo.results;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mknblch
 */
public class LouvainResult {

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodes;
    public final long iterations;
    public final long communityCount;
    public final List<Double> modularities;
    public final double modularity;

    private LouvainResult(long loadMillis, long computeMillis, long writeMillis, long nodes, long iterations,
                          long communityCount, double[] modularities, double modularity) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.iterations = iterations;
        this.communityCount = communityCount;

        this.modularities = new ArrayList<>(modularities.length);
        for (double mod : modularities) this.modularities.add(mod);


        this.modularity = modularity;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<LouvainResult> {

        private long nodes = 0;
        private long communityCount = 0;
        private long iterations = 1;
        private double[] modularities = new double[]{};
        private double modularity = -1.0;

        public Builder withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        public Builder withCommunityCount(long setCount) {
            this.communityCount = setCount;
            return this;
        }

        public Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder withModularities(double[] modularities) {
            this.modularities = modularities;
            return this;
        }

        public Builder withFinalModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }

        public LouvainResult build() {
            return new LouvainResult(loadDuration, evalDuration, writeDuration, nodes, iterations, communityCount,
                    modularities, modularity);
        }
    }
}
