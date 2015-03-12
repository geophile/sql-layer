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

import com.google.common.base.Objects;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class DagChecker<T> {
    protected abstract Set<? extends T> initialNodes();
    protected abstract Set<? extends T> nodesFrom(T starting);

    public boolean isDag() {
        DirectedGraph<T, Pair> graph = new DefaultDirectedGraph<>(Pair.class);

        Set<? extends T> initialNodes = initialNodes();
        Set<T> knownNodes = new HashSet<>(initialNodes.size() * 10); // just a guess
        Deque<T> nodePath = new ArrayDeque<>(20); // should be plenty
        boolean isDag = tryAdd(initialNodes, graph, knownNodes, new CycleDetector<>(graph), nodePath);
        if (!isDag) {
            this.badNodes = nodePath;
        }
        return isDag;
    }

    private boolean tryAdd(Set<? extends T> roots, Graph<T, Pair> graph, Set<T> knownNodes,
                           CycleDetector<T, Pair> cycleDetector, Deque<T> nodePath)
    {
        for (T node : roots) {
            nodePath.addLast(node);
            graph.addVertex(node);
            if (knownNodes.add(node)) {
                Set<? extends T> nodesFrom = nodesFrom(node);
                for (T from : nodesFrom) {
                    graph.addVertex(from);
                    Pair edge = new Pair(from, node);
                    graph.addEdge(from, node, edge);
                    nodePath.addLast(from);
                    if (cycleDetector.detectCycles())
                        return false;
                    nodePath.removeLast();
                }
                if (!tryAdd(nodesFrom, graph, knownNodes, cycleDetector, nodePath))
                    return false;
            }
            nodePath.removeLast();
        }
        return true;
    }

    public List<T> getBadNodePath() {
        return badNodes == null ? null : new ArrayList<>(badNodes);
    }

    private Deque<T> badNodes = null;

    private static class Pair {

        private Pair(Object from, Object to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Pair pair = (Pair) o;
            return Objects.equal(this.from,  pair.from) && Objects.equal(this.to, pair.to);
        }

        @Override
        public int hashCode() {
            int result = from != null ? from.hashCode() : 0;
            result = 31 * result + (to != null ? to.hashCode() : 0);
            return result;
        }

        private Object from;
        private Object to;
    }
}
