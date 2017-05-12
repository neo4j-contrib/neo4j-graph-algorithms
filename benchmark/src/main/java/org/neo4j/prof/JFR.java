package org.neo4j.prof;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.Aggregator;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ResultRole;
import org.openjdk.jmh.util.SingletonStatistics;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Runs Java Flight Recorder during JMH benchmarking.
 * <p>
 * To use: add {@code -prof org.neo4j.prof.JFR} to your JHM call.
 * <p>
 * The profiler unlock commercial JVM features.
 * It delays the recording for the amount of time that the warmup phase likely takes.
 * If your warmups don't run as estimated, the recording might start late or early.
 * <p>
 * Recordings are saved in a file per benchmark, the filename is printed at the end of each benchmark.
 */
public final class JFR implements ExternalProfiler {
    @Override
    public Collection<String> addJVMInvokeOptions(final BenchmarkParams params) {
        return Collections.emptySet();
    }

    @Override
    public Collection<String> addJVMOptions(final BenchmarkParams params) {
        final String fileName = getFileName(params);
        final IterationParams warmup = params.getWarmup();
        final long warmupSeconds = warmup
                .getTime()
                .convertTo(TimeUnit.SECONDS) * warmup.getCount();

        return Arrays.asList(
                "-XX:+UnlockCommercialFeatures",
                "-XX:+FlightRecorder",
                "-XX:StartFlightRecording=duration=0s,delay=" + warmupSeconds + "s,dumponexit=true,filename=" + fileName
        );
    }

    @Override
    public void beforeTrial(final BenchmarkParams benchmarkParams) {
        // do nothing
    }

    @Override
    public Collection<? extends Result> afterTrial(
            final BenchmarkResult br,
            final long pid,
            final File stdOut,
            final File stdErr) {
        final String fileName = getFileName(br.getParams());
        return Collections.singleton(
                new NoResult("JFR", "Results save to " + fileName));
    }

    @Override
    public boolean allowPrintOut() {
        return true;
    }

    @Override
    public boolean allowPrintErr() {
        return true;
    }

    @Override
    public String getDescription() {
        return "runs the flight recorder alongside the benchmark";
    }

    private String getFileName(final BenchmarkParams params) {
        return params.id() + ".jfr";
    }

    private static final class NoResult extends Result<NoResult> {

        private final String label;
        private final String output;

        private NoResult(final String label, final String output) {
            super(
                    ResultRole.SECONDARY,
                    label,
                    new SingletonStatistics(Double.NaN),
                    "N/A",
                    AggregationPolicy.SUM);
            this.label = label;
            this.output = output;
        }

        String output() {
            return output;
        }

        @Override
        protected Aggregator<NoResult> getThreadAggregator() {
            return new NoResultAggregator();
        }

        @Override
        protected Aggregator<NoResult> getIterationAggregator() {
            return new NoResultAggregator();
        }

        @Override
        public String extendedInfo() {
            return output;
        }
    }

    private static final class NoResultAggregator implements Aggregator<NoResult> {
        @Override
        public NoResult aggregate(final Collection<NoResult> results) {
            return new NoResult(
                    results.iterator().next().label,
                    results.stream()
                            .map(NoResult::output)
                            .collect(Collectors.joining(System.lineSeparator()))
            );
        }
    }
}
