package org.neo4j.graphalgo.similarity;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class WeightsTest {

    @Test
    public void shouldTransformListToArray() throws Exception {
        Number[] values = {1.0, 2.0, 3.0, 4.0};
        List<Number> weightList = Arrays.asList(values);
        assertArrayEquals(new double[]{1.0, 2.0, 3.0, 4.0}, Weights.buildWeights(weightList), 0.01);
    }

    @Test
    public void nans() throws Exception {
        Number[] values = {Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN};
        List<Number> weightList = Arrays.asList(values);
        double[] actuals = Weights.buildRleWeights(weightList, 3);
        System.out.println("actuals = " + Arrays.toString(actuals));
        assertArrayEquals(new double[]{

                Double.POSITIVE_INFINITY, 5.0, Double.NaN}, actuals, 0.01);
    }

    @Test
    public void rleWithOneRepeatedValue() throws Exception {
        Number[] values = {4.0, 4.0};
        List<Number> weightList = Arrays.asList(values);
        double[] actuals = Weights.buildRleWeights(weightList, 1);
        System.out.println("actuals = " + Arrays.toString(actuals));
        assertArrayEquals(new double[]{Double.POSITIVE_INFINITY, 2.0, 4.0}, actuals, 0.01);
    }

    @Test
    public void rleWithMoreThanOneRepeatedValue() throws Exception {
        Number[] values = {2.0, 2.0, 4.0, 4.0, 6.0, 6.0};
        List<Number> weightList = Arrays.asList(values);
        double[] actuals = Weights.buildRleWeights(weightList, 1);
        System.out.println("actuals = " + Arrays.toString(actuals));
        assertArrayEquals(new double[]{
                Double.POSITIVE_INFINITY, 2.0, 2.0,
                Double.POSITIVE_INFINITY, 2.0, 4.0,
                Double.POSITIVE_INFINITY, 2.0, 6.0}, actuals, 0.01);
    }

    @Test
    public void rleWithMoreThanOneRepeatedValueOfDifferentSizes() throws Exception {
        Number[] values = {2.0, 2.0, 4.0, 4.0, 4.0, 4.0, 6.0, 6.0};
        List<Number> weightList = Arrays.asList(values);
        double[] actuals = Weights.buildRleWeights(weightList, 1);
        System.out.println("actuals = " + Arrays.toString(actuals));
        assertArrayEquals(new double[]{
                Double.POSITIVE_INFINITY, 2.0, 2.0,
                Double.POSITIVE_INFINITY, 4.0, 4.0,
                Double.POSITIVE_INFINITY, 2.0, 6.0}, actuals, 0.01);
    }

    @Test
    public void rleWithMixedValues() throws Exception {
        Number[] values = {7.0, 2.0, 2.0, 4.0, 4.0, 6.0, 6.0, 7.0};
        List<Number> weightList = Arrays.asList(values);
        double[] actuals = Weights.buildRleWeights(weightList, 1);
        System.out.println("actuals = " + Arrays.toString(actuals));
        assertArrayEquals(new double[]{
                7.0,
                Double.POSITIVE_INFINITY, 2.0, 2.0,
                Double.POSITIVE_INFINITY, 2.0, 4.0,
                Double.POSITIVE_INFINITY, 2.0, 6.0,
                7.0}, actuals, 0.01);
    }

    @Test
    public void rleWithNoRepeats() throws Exception {
        Number[] values = {7.0, 2.0, 2.0, 4.0, 4.0, 6.0, 6.0, 7.0};
        List<Number> weightList = Arrays.asList(values);
        double[] actuals = Weights.buildRleWeights(weightList, 5);
        assertArrayEquals(new double[]{7.0, 2.0, 2.0, 4.0, 4.0, 6.0, 6.0, 7.0}, actuals, 0.01);
    }

    @Test
    public void rleWithEmptyArray() throws Exception {
        List<Number> weightList = Collections.emptyList();
        double[] actuals = Weights.buildRleWeights(weightList, 5);
        assertArrayEquals(new double[0], actuals, 0.01);
    }

}
