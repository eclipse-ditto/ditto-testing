/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.ditto.testing.system.client.util;

/**
 * Simple StopWatch that can be used in tests.
 */
public class StopWatch {

    long start;
    long stop;

    /**
     * Creates a StopWatch and starts it.
     */
    public StopWatch() {
        start = System.currentTimeMillis();
    }

    /**
     * @return new StopWatch
     */
    public static StopWatch start() {
        return new StopWatch();
    }

    /**
     * Stops the stopwatch and returns the duration.
     */
    public long stop() {
        stop = System.currentTimeMillis();
        return stop - start;
    }

    /**
     * @return the measured duation or -1 if the stopwatch was not stopped yet
     */
    public long duration() {

        if (stop > start) {

            return stop - start;
        } else {
            return -1;
        }
    }

    /**
     * @param items number of items processed
     * @return throughput in items/second
     */
    public double throughput(final long items) {
        if (stop > start) {
            return ((double) items) / (stop - start) * 1000;
        } else {
            return 0;
        }
    }

    /**
     * Gets start timestamp.
     *
     * @return the start timestamp
     */
    public long getStart() {
        return start;
    }

    /**
     * Gets stop timestamp.
     *
     * @return the stop timestamp
     */
    public long getStop() {
        return stop;
    }
}
