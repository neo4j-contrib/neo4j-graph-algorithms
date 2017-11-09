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

/**
 * @author mknblch
 */
public class SCCResult {

    public final Long loadMillis;
    public final Long computeMillis;
    public final Long writeMillis;
    public final Long setCount;
    public final Long minSetSize;
    public final Long maxSetSize;

    public SCCResult(Long loadMillis,
                     Long computeMillis,
                     Long writeMillis,
                     Long setCount,
                     Long minSetSize,
                     Long maxSetSize) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.setCount = setCount;
        this.minSetSize = minSetSize;
        this.maxSetSize = maxSetSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends AbstractResultBuilder<SCCResult> {

        private long setCount;
        private long minSetSize;
        private long maxSetSize;

        public Builder withSetCount(long setCount) {
            this.setCount = setCount;
            return this;
        }

        public Builder withMinSetSize(long minSetSize) {
            this.minSetSize = minSetSize;
            return this;
        }

        public Builder withMaxSetSize(long maxSetSize) {
            this.maxSetSize = maxSetSize;
            return this;
        }

        @Override
        public SCCResult build() {
            return new SCCResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    setCount,
                    minSetSize,
                    maxSetSize);
        }
    }

}
