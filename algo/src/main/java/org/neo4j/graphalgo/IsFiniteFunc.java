package org.neo4j.graphalgo;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class IsFiniteFunc {

    @UserFunction("algo.isFinite")
    @Description("CALL algo.isFinite(value) - return true iff the given argument is a finite value (not ±Infinity, NaN, or null), false otherwise.")
    public boolean isFinite(@Name(value = "value") Number value) {
        return value != null && Double.isFinite(value.doubleValue());
    }

    @UserFunction("algo.isInfinite")
    @Description("CALL algo.isInfinite(value) - return true iff the given argument is not a finite value (±Infinity, NaN, or null), false otherwise.")
    public boolean isInfinite(@Name(value = "value") Number value) {
        return value == null || !Double.isFinite(value.doubleValue());
    }

    @UserFunction("algo.Infinity")
    @Description("CALL algo.Infinity() - returns Double.POSITIVE_INFINITY as a value.")
    public double Infinity() {
        return Double.POSITIVE_INFINITY;
    }

    @UserFunction("algo.NaN")
    @Description("CALL algo.NaN() - returns Double.NaN as a value.")
    public double NaN() {
        return Double.NaN;
    }
}
