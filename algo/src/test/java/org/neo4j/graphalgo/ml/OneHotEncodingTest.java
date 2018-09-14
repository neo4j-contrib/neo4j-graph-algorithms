package org.neo4j.graphalgo.ml;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class OneHotEncodingTest {

    @Test
    public void singleCategorySelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Collections.singletonList("Italian");

        assertEquals(asList(1L, 0L, 0L), new OneHotEncoding().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void noCategoriesSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Collections.emptyList();

        assertEquals(asList(0L, 0L, 0L), new OneHotEncoding().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void moreThanOneSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("Italian", "Chinese");

        assertEquals(asList(1L, 0L, 1L), new OneHotEncoding().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void allSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("Italian", "Chinese", "Indian");

        assertEquals(asList(1L, 1L, 1L), new OneHotEncoding().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void nonExistentSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Collections.singletonList("British");

        assertEquals(asList(0L, 0L, 0L), new OneHotEncoding().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void oneNonExistentSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("British", "Chinese");

        assertEquals(asList(0L, 0L, 1L), new OneHotEncoding().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void nullSelectedMeansNoneSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");

        assertEquals(asList(0L, 0L, 0L), new OneHotEncoding().oneHotEncoding(values, null));
    }

    @Test
    public void nullAvailableMeansEmptyArray() {
        assertEquals(Collections.emptyList(), new OneHotEncoding().oneHotEncoding(null, null));
    }

}