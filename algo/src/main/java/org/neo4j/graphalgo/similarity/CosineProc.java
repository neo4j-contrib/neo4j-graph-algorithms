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

public class CosineProc extends SimilarityProc {

    @Procedure(name = "algo.similarity.cosine.stream", mode = Mode.READ)
    @Description("CALL algo.similarity.cosine.stream([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD item1, item2, count1, count2, intersection, similarity - computes cosine distance")
    // todo count1,count2 = could be the non-null values, intersection the values where both are non-null?
    public Stream<SimilarityResult> cosineStream(
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Double skipValue = configuration.get("skipValue", null);

        WeightedInput[] inputs = prepareWeights(rawData, configuration, skipValue);

        double similarityCutoff = similarityCutoff(configuration);
        int topN = getTopN(configuration);
        int topK = getTopK(configuration);
        SimilarityComputer<WeightedInput> computer = similarityComputer(skipValue);

        return generateStream(configuration, inputs, similarityCutoff, topN, topK, computer);
    }

    @Procedure(name = "algo.similarity.cosine", mode = Mode.WRITE)
    @Description("CALL algo.similarity.cosine([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD p50, p75, p90, p99, p999, p100 - computes cosine similarities")
    public Stream<SimilaritySummaryResult> cosine(
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Double skipValue = configuration.get("skipValue", null);

        WeightedInput[] inputs = prepareWeights(rawData, configuration, skipValue);

        double similarityCutoff = similarityCutoff(configuration);
        int topN = getTopN(configuration);
        int topK = getTopK(configuration);
        SimilarityComputer<WeightedInput> computer = similarityComputer(skipValue);
        Stream<SimilarityResult> stream = generateStream(configuration, inputs, similarityCutoff, topN, topK, computer);

        boolean write = configuration.isWriteFlag(false) && (similarityCutoff > 0.0 || topK > 0);
        return writeAndAggregateResults(configuration, stream, inputs.length, write, "SIMILAR");
    }

    private Stream<SimilarityResult> generateStream(ProcedureConfiguration configuration, WeightedInput[] inputs,
                                                    double similarityCutoff, int topN, int topK,
                                                    SimilarityComputer<WeightedInput> computer) {
        int size = inputs[0].initialSize;

        Supplier<RleDecoder> decoderFactory = createDecoderFactory(configuration.getGraphName("dense"), size);

        return topN(similarityStream(inputs, computer, configuration, decoderFactory, similarityCutoff, topK), topN)
                .map(SimilarityResult::squareRooted);
    }

    private SimilarityComputer<WeightedInput> similarityComputer(Double skipValue) {
        return skipValue == null ?
                (decoder, s, t, cutoff) -> s.cosineSquares(decoder, cutoff, t) :
                (decoder, s, t, cutoff) -> s.cosineSquaresSkip(decoder, cutoff, t, skipValue);
    }

    private double similarityCutoff(ProcedureConfiguration configuration) {
        double similarityCutoff = getSimilarityCutoff(configuration);
        // as we don't compute the sqrt until the end
        if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;
        return similarityCutoff;
    }


}
