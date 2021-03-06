/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2009-2015 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.foundationdb.util;

import org.jgrapht.EdgeFactory;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class DagCheckerTest {

    @Test
    public void empty() {
        new GraphFactory().isDag();
    }

    @Test
    public void line() {
        new GraphFactory().connect("a", "b").connect("b", "c").isDag();
    }

    @Test
    public void circle() {
        new GraphFactory().connect("a", "b").connect("b", "a").notDag("a", "b", "a");
    }

    @Test
    public void circlePlusShortCircuit() {
        new GraphFactory()
                .connect("a", "b")
                .connect("b", "c")
                .connect("c", "d")
                .connect("b", "d")
                .connect("d", "a")
                .notDag("a", "b", "c", "d", "a");
    }

    @Test
    public void split() {
        new GraphFactory()
                .connect("a", "b1").connect("b1", "c")
                .connect("a", "b2").connect("b2", "c")
                .isDag();
    }

    private static class GraphFactory {

        public GraphFactory connect(String from, String to) {
            graph.addVertex(from);
            graph.addVertex(to);
            graph.addEdge(from, to);
            return this;
        }

        public void isDag() {
            DagChecker<String> checker = getChecker();
            assertTrue("expected DAG for " + graph, checker.isDag());
            assertEquals("bad nodes", null, checker.getBadNodePath());
        }

        public void notDag(String... vertices) {
            DagChecker<String> checker = getChecker();
            assertFalse("expected non-DAG for " + graph, checker.isDag());
            List<String> expectedList = Arrays.asList(vertices);
            assertEquals("cycle path", expectedList, checker.getBadNodePath());
        }

        private DagChecker<String> getChecker() {
            return new DagChecker<String>() {
                @Override
                protected Set<? extends String> initialNodes() {
                    return new TreeSet<>(graph.vertexSet());
                }

                @Override
                protected Set<? extends String> nodesFrom(String starting) {
                    Set<String> candidates = new TreeSet<>(graph.vertexSet());
                    for (Iterator<String> iter = candidates.iterator(); iter.hasNext(); ) {
                        String vertex = iter.next();
                        if (vertex.equals(starting) || (!graph.containsEdge(starting, vertex)))
                            iter.remove();
                    }
                    return candidates;
                }
            };
        }

        private Graph<String, StringPair> graph = new DefaultDirectedGraph<>(factory);
    }

    private static final EdgeFactory<String, StringPair> factory = new EdgeFactory<String, StringPair>() {
        @Override
        public StringPair createEdge(String s, String s1) {
            return new StringPair(s, s1);
        }
    };

    private static class StringPair {

        @Override
        public String toString() {
            return from + " -> " + to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringPair that = (StringPair) o;
            return from.equals(that.from) && to.equals(that.to);

        }

        @Override
        public int hashCode() {
            int result = from.hashCode();
            result = 31 * result + to.hashCode();
            return result;
        }

        private StringPair(String from, String to) {
            this.from = from;
            this.to = to;
        }

        private String from;
        private String to;
    }
}
