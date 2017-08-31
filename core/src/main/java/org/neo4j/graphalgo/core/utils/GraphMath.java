package org.neo4j.graphalgo.core.utils;

/**
 * @author mknblch
 */
public class GraphMath {

    // iterative binomial coefficient nCk (n choose k)
    public static double binomial(long n, long k) {
        if (k > n) {
            return 0;
        } else {
            int a = 1, b = 1;
            for (long i = n - k + 1; i <= n; i++) {
                a *= i;
            }
            for (long i = 2; i <= k; i++) {
                b *= i;
            }
            return (double) a / b;
        }
    }

    public static double test(long n) {
        int b = 1;

        for (long i = 2; i <= 2; i++) {
            b *= i;
        }

        return b;
    }


    // iterative binomial coefficient nC2 (n choose 2)
    public static double binomial2(long n) {
        if (n < 2) {
            return 0;
        } else {
            int a = 1;
            for (long i = n - 1; i <= n; i++) {
                a *= i;
            }
            return (double) a / 2d;
        }
    }


}
