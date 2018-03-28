/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.utils.ProgressLogger;

import java.util.function.Supplier;

/**
 * @author mknblch
 */
public class TestProgressLogger implements ProgressLogger {

    public static final ProgressLogger INSTANCE = new TestProgressLogger();

    @Override
    public void logProgress(double percentDone, Supplier<String> msg) {
        System.out.printf("%.0f%% (%s)%n", percentDone * 100, msg.get());
    }

    @Override
    public void logDone(Supplier<String> msg) {
        System.out.println(msg.get());
    }
}
