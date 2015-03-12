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
package com.foundationdb.sql.server;

import com.foundationdb.ais.model.Routine;

import java.util.ArrayDeque;
import java.util.Deque;

public class ServerCallContextStack
{
    private final Deque<Entry> stack = new ArrayDeque<>();
    private final Deque<ServerSessionBase> callees = new ArrayDeque<>();
    private ServerTransaction sharedTransaction;
    private boolean firstCalleeNested;

    private ServerCallContextStack() {
    }

    public static class Entry {
        private ServerQueryContext context;
        private ServerRoutineInvocation invocation;

        private Entry(ServerQueryContext context, ServerRoutineInvocation invocation) {
            this.context = context;
            this.invocation = invocation;
        }

        public ServerQueryContext getContext() {
            return context;
        }
        
        public ServerRoutineInvocation getInvocation() {
            return invocation;
        }

        public Routine getRoutine() {
            return invocation.getRoutine();
        }
    }

    private static final ThreadLocal<ServerCallContextStack> tl = new ThreadLocal<ServerCallContextStack>() {
        @Override
        protected ServerCallContextStack initialValue() {
            return new ServerCallContextStack();
        }
    };

    public static ServerCallContextStack get() {
        return tl.get();
    }
    
    /** Convenience for use by Routines. */
    public static ServerQueryContext getCallingContext() {
        return get().current().getContext();
    }

    public Entry current() {
        return stack.peekLast();
    }
    
    public void push(ServerQueryContext context, ServerRoutineInvocation invocation) {
        stack.addLast(new Entry(context, invocation));
    }
    
    public void pop(ServerQueryContext context, ServerRoutineInvocation invocation,
                    boolean success) {
        Entry last = stack.removeLast();
        assert (last.getContext() == context);
        if (stack.isEmpty()) {
            if (firstCalleeNested) {
                // Because ResultSets can be returned while still open, we
                // cannot close everything down until after the last call.
                while (!callees.isEmpty()) {
                    ServerSessionBase callee = callees.pop();
                    boolean active = callee.endCall(context, invocation, true, success);
                    assert !active : callee;
                }
                if (sharedTransaction != null) {
                    // We took over transaction from a nested call.
                    if (success) {
                        sharedTransaction.commit();
                    }
                    else {
                        sharedTransaction.rollback();
                    }
                    sharedTransaction = null;
                }
            }
            else {
                // This is the case where an embedded connection was made by
                // Java code, so there's no easy way to track its end-of-live.
                callees.clear();
                assert (sharedTransaction == null);
            }
        }
        else {
            while (true) {
                ServerSessionBase callee = callees.peek();
                if (callee == null) break;
                boolean active = callee.endCall(context, invocation, false, success);
                if (active) {
                    // If something is still open from this one, its transaction
                    // will need to be used by any subsequent ones, too.
                    if (sharedTransaction == null) {
                        sharedTransaction = callee.transaction;
                    }
                    break;
                }
                callees.pop();
            }
        }
    }

    public void addCallee(ServerSessionBase callee) {
        if (callees.isEmpty()) {
            firstCalleeNested = !stack.isEmpty();
        }
        callees.push(callee);
    }

    public void endCallee(ServerSessionBase callee) {
        if (stack.isEmpty() && !firstCalleeNested) {
            callees.clear();
            assert (sharedTransaction == null);
        }
    }

    public ServerSessionBase currentCallee() {
        return callees.peek();
    }

    public ServerTransaction getSharedTransaction() {
        return sharedTransaction;
    }
}
