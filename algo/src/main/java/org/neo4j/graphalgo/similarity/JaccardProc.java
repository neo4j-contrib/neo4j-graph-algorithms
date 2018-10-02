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

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.impl.util.TopKConsumer.topK;

public class JaccardProc extends SimilarityProc {

    @Procedure(name = "algo.similarity.jaccard.stream", mode = Mode.READ)
    @Description("CALL algo.similarity.jaccard.stream([{item:id, categories:[ids]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD item1, item2, count1, count2, intersection, similarity - computes jaccard similarities")
    public Stream<SimilarityResult> similarityStream(
            @Name(value = "data", defaultValue = "null") List<Map<String,Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        SimilarityComputer<CategoricalInput> computer = (s, t, cutoff) -> s.jaccard(cutoff, t);

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        CategoricalInput[] inputs = prepareCategories(data, getDegreeCutoff(configuration));

        return topN(similarityStream(inputs, computer, configuration, getSimilarityCutoff(configuration), getTopK(configuration)), getTopN(configuration));
    }

    @Procedure(name = "algo.similarity.jaccard", mode = Mode.WRITE)
    @Description("CALL algo.similarity.jaccard([{item:id, categories:[ids]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD p50, p75, p90, p99, p999, p100 - computes jaccard similarities")
    public Stream<SimilaritySummaryResult> jaccard(
            @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        SimilarityComputer<CategoricalInput> computer = (s,t,cutoff) -> s.jaccard(cutoff, t);

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        CategoricalInput[] inputs = prepareCategories(data, getDegreeCutoff(configuration));

        double similarityCutoff = getSimilarityCutoff(configuration);
        Stream<SimilarityResult> stream = topN(similarityStream(inputs, computer, configuration, similarityCutoff, getTopK(configuration)), getTopN(configuration));

        boolean write = configuration.isWriteFlag(false) && similarityCutoff > 0.0;
        return writeAndAggregateResults(configuration, stream, inputs.length, write);
    }


}
