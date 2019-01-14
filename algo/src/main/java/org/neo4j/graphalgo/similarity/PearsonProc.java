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
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class PearsonProc extends SimilarityProc {

    @Procedure(name = "algo.similarity.pearson.stream", mode = Mode.READ)
    @Description("CALL algo.similarity.pearson.stream([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD item1, item2, count1, count2, intersection, similarity - computes cosine distance")
    // todo count1,count2 = could be the non-null values, intersection the values where both are non-null?
    public Stream<SimilarityResult> pearsonStream(
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Double skipValue = configuration.get("skipValue", null);

        SimilarityComputer<WeightedInput> computer = similarityComputer(skipValue);

        WeightedInput[] inputs = prepareWeights(rawData, configuration, skipValue);

        if(inputs.length == 0) {
            return Stream.empty();
        }

        double similarityCutoff = similarityCutoff(configuration);
        int topN = getTopN(configuration);
        int topK = getTopK(configuration);

        return generateWeightedStream(configuration, inputs, similarityCutoff, topN, topK, computer);
    }

    @Procedure(name = "algo.similarity.pearson", mode = Mode.WRITE)
    @Description("CALL algo.similarity.pearson([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD p50, p75, p90, p99, p999, p100 - computes cosine similarities")
    public Stream<SimilaritySummaryResult> pearson(
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Double skipValue = configuration.get("skipValue", null);
        SimilarityComputer<WeightedInput> computer = similarityComputer(skipValue);

        WeightedInput[] inputs = prepareWeights(rawData, configuration, skipValue);

        String writeRelationshipType = configuration.get("writeRelationshipType", "SIMILAR");
        String writeProperty = configuration.getWriteProperty("score");
        if(inputs.length == 0) {
            return emptyStream(writeRelationshipType, writeProperty);
        }

        double similarityCutoff = similarityCutoff(configuration);
        int topN = getTopN(configuration);
        int topK = getTopK(configuration);

        Stream<SimilarityResult> stream = generateWeightedStream(configuration, inputs, similarityCutoff, topN, topK, computer);

        boolean write = configuration.isWriteFlag(false) && similarityCutoff > 0.0;
        return writeAndAggregateResults(stream, inputs.length, write, writeRelationshipType, writeProperty);
    }

    private SimilarityComputer<WeightedInput> similarityComputer(Double skipValue) {
        return skipValue == null ?
                (decoder, s, t, cutoff) -> s.pearson(decoder, cutoff, t) :
                (decoder, s, t, cutoff) -> s.pearsonSkip(decoder, cutoff, t, skipValue);
    }

    private double similarityCutoff(ProcedureConfiguration configuration) {
        return getSimilarityCutoff(configuration);
    }

    Stream<SimilarityResult> generateWeightedStream(ProcedureConfiguration configuration, WeightedInput[] inputs,
                                                    double similarityCutoff, int topN, int topK,
                                                    SimilarityComputer<WeightedInput> computer) {
        Supplier<RleDecoder> decoderFactory = createDecoderFactory(configuration, inputs[0]);
        return topN(similarityStream(inputs, computer, configuration, decoderFactory, similarityCutoff, topK), topN);
    }


}
