package org.neo4j.graphalgo.similarity;

import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@RunWith(JUnitQuickcheck.class)
public class RleReaderTest {
    @Test
    public void nothingRepeats() throws Exception {
        // given
        List<Number> vector1List = asList(5.0, 4.0, 5.0, 4.0, 5.0);

        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);

        // then
        RleReader rleReader = new RleReader(vector1Rle);

        for (Number value : vector1List) {
            rleReader.next();
            assertEquals(value.doubleValue(), rleReader.value(), 0.001);
        }
    }

    @Test
    public void everythingRepeats() throws Exception {
        // given
        List<Number> vector1List = asList(5.0, 5.0, 5.0, 5.0, 5.0);

        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);

        // then
        RleReader rleReader = new RleReader(vector1Rle);

        for (Number value : vector1List) {
            rleReader.next();
            assertEquals(value.doubleValue(), rleReader.value(), 0.001);
        }
    }

    @Test
    public void mixedRepeats() throws Exception {
        // given
        List<Number> vector1List = asList(5.0, 5.0, 5.0, 5.0, 5.0, 4.0, 5.0);

        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
        System.out.println(Arrays.toString(vector1Rle));

        // then
        RleReader rleReader = new RleReader(vector1Rle);

        for (Number value : vector1List) {
            rleReader.next();
            assertEquals(value.doubleValue(), rleReader.value(), 0.001);
        }
    }


}
