package sketches.correlation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import sketches.correlation.PearsonCorrelation.ConfidenceInterval;

public class PearsonCorrelationTest {

    static final double delta = 0.001;

    @Test
    public void shouldComputeOneTailedTTestPValue() {
        assertEquals(0.24302101, PearsonCorrelation.pValueOneTailed(0.25, 10), delta);
        assertEquals(0.07055664, PearsonCorrelation.pValueOneTailed(0.50, 10), delta);
        assertEquals(0.14388153, PearsonCorrelation.pValueOneTailed(0.25, 20), delta);
        assertEquals(0.01238478, PearsonCorrelation.pValueOneTailed(0.50, 20), delta);
    }

    @Test
    public void shouldComputeTwoTailedTTestPValue() {
        assertEquals(0.48604202, PearsonCorrelation.pValueTwoTailed(0.25, 10), delta);
        assertEquals(0.14111328, PearsonCorrelation.pValueTwoTailed(0.50, 10), delta);
        assertEquals(0.28776307, PearsonCorrelation.pValueTwoTailed(0.25, 20), delta);
        assertEquals(0.02476956, PearsonCorrelation.pValueTwoTailed(0.50, 20), delta);
    }

    @Test
    public void shouldComputeIfCorrelationCoefficientIsSignificant() {
        assertFalse(PearsonCorrelation.isSignificant(.50, 10, 0.05));
        assertTrue(PearsonCorrelation.isSignificant(.50, 100, 0.05));
    }

    @Test
    public void shouldComputeConfidenceIntervals1() {
        ConfidenceInterval interval = PearsonCorrelation
                .confidenceInterval(0.5, 10, .95);
        assertEquals(-0.189, interval.lowerBound, delta);
        assertEquals(0.859, interval.upperBound, delta);
    }

    @Test
    public void shouldComputeConfidenceIntervals2() {
        ConfidenceInterval interval = PearsonCorrelation
                .confidenceInterval(0.5, 10, .80);
        assertEquals(0.065, interval.lowerBound, delta);
        assertEquals(0.775, interval.upperBound, delta);
    }

    @Test
    public void shouldComputeConfidenceIntervals3() {
        ConfidenceInterval interval = PearsonCorrelation
                .confidenceInterval(-0.654, 34, .95);
        assertEquals(-0.812, interval.lowerBound, delta);
        assertEquals(-0.406, interval.upperBound, delta);

    }

}
