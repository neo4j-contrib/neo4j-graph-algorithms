package org.neo4j.graphalgo.similarity;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Precision;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitQuickcheck.class)
public class RleReaderPropertyBasedTest {

    @Property
    public void mixedRepeats(List<@InRange(min = "0", max = "2") @Precision(scale=0) Number> vector1List, @InRange(min="1", max="3") int limit) throws Exception {
        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, limit);
        System.out.println(vector1List);
        System.out.println(Arrays.toString(vector1Rle));

        // then
        RleReader rleReader = new RleReader(vector1Rle);

        for (Number value : vector1List) {
            rleReader.next();
            assertEquals(value.doubleValue(), rleReader.value(), 0.001);
        }
    }
}
