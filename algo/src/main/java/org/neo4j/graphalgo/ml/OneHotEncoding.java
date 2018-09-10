package org.neo4j.graphalgo.ml;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class OneHotEncoding {

    @UserFunction("algo.ml.oneHotEncoding")
    @Description("CALL algo.ml.oneHotEncoding(availableValues, selectedValues) - return a list of selected values in a one hot encoding format.")
    public List<Long> oneHotEncoding(@Name(value = "availableValues") List<Object> availableValues,
                                     @Name(value = "selectedValues") List<Object> selectedValues) {
        if (availableValues == null) {
            return LongStream.empty().boxed().collect(Collectors.toList());
        }

        if (selectedValues == null) {
            return LongStream.range(0, availableValues.size()).map(index -> 0).boxed().collect(Collectors.toList());
        }

        Set<Object> selectedValuesSet = new HashSet<>(selectedValues);
        Object[] availableValuesArray = availableValues.toArray();
        return LongStream.range(0, availableValues.size())
                .map(index -> selectedValuesSet.contains(availableValuesArray[(int) index]) ? 1L : 0L)
                .boxed()
                .collect(Collectors.toList());
    }
}
