package org.neo4j.graphalgo.impl.results;

import org.junit.Test;
import org.neo4j.graphalgo.Normalization;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NormalizedCentralityResultTest {
    @Test
    public void maxNormalization() {
        CentralityResult centralityResult = mock(CentralityResult.class);
        when(centralityResult.score(0)).thenReturn(1.0);
        when(centralityResult.score(1)).thenReturn(2.0);
        when(centralityResult.score(2)).thenReturn(3.0);
        when(centralityResult.score(3)).thenReturn(4.0);
        when(centralityResult.computeMax()).thenReturn(4.0);

        CentralityResult normalizedResult = Normalization.MAX.apply(centralityResult);

        assertEquals(0.25, normalizedResult.score(0), 0.01);
        assertEquals(0.5, normalizedResult.score(1), 0.01);
        assertEquals(0.75, normalizedResult.score(2), 0.01);
        assertEquals(1.0, normalizedResult.score(3), 0.01);
    }

    @Test
    public void noNormalization() {
        CentralityResult centralityResult = mock(CentralityResult.class);
        when(centralityResult.score(0)).thenReturn(1.0);
        when(centralityResult.score(1)).thenReturn(2.0);
        when(centralityResult.score(2)).thenReturn(3.0);
        when(centralityResult.score(3)).thenReturn(4.0);
        when(centralityResult.computeMax()).thenReturn(4.0);

        CentralityResult normalizedResult = Normalization.NONE.apply(centralityResult);

        assertEquals(1.0, normalizedResult.score(0), 0.01);
        assertEquals(2.0, normalizedResult.score(1), 0.01);
        assertEquals(3.0, normalizedResult.score(2), 0.01);
        assertEquals(4.0, normalizedResult.score(3), 0.01);
    }

    @Test
    public void l2Norm() {
        CentralityResult centralityResult = mock(CentralityResult.class);
        when(centralityResult.score(0)).thenReturn(1.0);
        when(centralityResult.score(1)).thenReturn(2.0);
        when(centralityResult.score(2)).thenReturn(3.0);
        when(centralityResult.score(3)).thenReturn(4.0);
        when(centralityResult.computeL2Norm()).thenReturn(4.0);

        CentralityResult normalizedResult = Normalization.L2NORM.apply(centralityResult);

        assertEquals(0.25, normalizedResult.score(0), 0.01);
        assertEquals(0.5, normalizedResult.score(1), 0.01);
        assertEquals(0.75, normalizedResult.score(2), 0.01);
        assertEquals(1.0, normalizedResult.score(3), 0.01);
    }

    @Test
    public void l1Norm() {
        CentralityResult centralityResult = mock(CentralityResult.class);
        when(centralityResult.score(0)).thenReturn(1.0);
        when(centralityResult.score(1)).thenReturn(2.0);
        when(centralityResult.score(2)).thenReturn(3.0);
        when(centralityResult.score(3)).thenReturn(4.0);
        when(centralityResult.computeL1Norm()).thenReturn(4.0);

        CentralityResult normalizedResult = Normalization.L1NORM.apply(centralityResult);

        assertEquals(0.25, normalizedResult.score(0), 0.01);
        assertEquals(0.5, normalizedResult.score(1), 0.01);
        assertEquals(0.75, normalizedResult.score(2), 0.01);
        assertEquals(1.0, normalizedResult.score(3), 0.01);
    }
}