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
package com.foundationdb.server.service.session;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Session implements AutoCloseable
{
    private final static long UNSET_NANOS = -1;

    private final static AtomicLong idGenerator = new AtomicLong(0);

    private final Map<Key<?>,Object> map = new HashMap<>();
    private final SessionEventListener listener;
    private final long sessionId = idGenerator.getAndIncrement();
    private volatile boolean cancelCurrentQuery = false;
    private long startMarkerNanos =  UNSET_NANOS;
    private long timeoutAfterNanos = UNSET_NANOS;

    public String toString()
    {
        return String.format("Session(%s)", sessionId);
    }

    Session(SessionEventListener listener) {
        this.listener = listener;
    }

    public long sessionId() {
        return sessionId;
    }

    public <T> T get(Session.Key<T> key) {
        return cast(key, map.get(key));
    }

    public <T> T put(Session.Key<T> key, T item) {
        return cast(key, map.put(key, item));
    }

    public <T> T remove(Key<T> key) {
        return cast(key, map.remove(key));
    }

    public <K,V> V get(MapKey<K,V> mapKey, K key) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    public <K,V> V put(MapKey<K,V> mapKey, K key, V value) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            map = new HashMap<>();
            put(mapKey.asKey(), map);
        }
        return map.put(key, value);
    }

    public <K,V> V remove(MapKey<K,V> mapKey, K key) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            return null;
        }
        return map.remove(key);
    }

    public <T> void push(StackKey<T> key, T item) {
        Deque<T> deque = get( key.asKey() );
        if (deque == null) {
            deque = new ArrayDeque<>();
            put(key.asKey(), deque);
        }
        deque.offerLast(item);
    }

    public <T> T pop(StackKey<T> key) {
        Deque<T> deque = get( key.asKey() );
        if (deque == null) {
            return null;
        }
        return deque.pollLast();
    }
    
    public boolean isEmpty(StackKey<?> key) {
        Deque<?> deque = get( key.asKey() );
        return deque == null || deque.isEmpty();
    }

    // "unused" suppression: Key<T> is only used for type inference
    // "unchecked" suppression: we know from the put methods that Object will be of type T
    @SuppressWarnings({"unused", "unchecked"})
    private static <T> T cast(Key<T> key, Object o) {
        return (T) o;
    }

    @Override
    public void close()
    {
        if (listener != null) {
            listener.sessionClosing();
        }
        // For now do nothing to any cached resources.
        // Later, we'll close any "resource" that is added to the session.
        //
        map.clear();
    }

    public void cancelCurrentQuery(boolean cancel)
    {
        cancelCurrentQuery = cancel;
    }

    public boolean isCurrentQueryCanceled()
    {
        return cancelCurrentQuery;
    }

    private void requireTimeoutAfterSet() {
        if(!hasTimeoutAfterNanos()) {
            throw new IllegalStateException("Timeout nanos not set");
        }
    }
    public boolean hasTimeoutAfterNanos() {
        return timeoutAfterNanos != UNSET_NANOS;
    }

    public long getElapsedMillis() {
        requireTimeoutAfterSet();
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startMarkerNanos);
    }

    public long getRemainingNanosBeforeTimeout() {
        requireTimeoutAfterSet();
        return timeoutAfterNanos - System.nanoTime();
    }

    public void setTimeoutAfterMillis(long millis) {
        if(millis < 0) {
            this.startMarkerNanos = this.timeoutAfterNanos = UNSET_NANOS;
        } else {
            this.startMarkerNanos = System.nanoTime();
            this.timeoutAfterNanos = startMarkerNanos + TimeUnit.MILLISECONDS.toNanos(millis);
        }
    }

    @SuppressWarnings("unused") // for <T> parameter; it's only useful for compile-time checking
    public static class Key<T> {
        private final Class<?> owner;
        private final String name;

        /** Create a new Key with the given name. Looks up owning class, see {@link #named(String,Class)}. **/
        public static <T> Key<T> named(String name) {
            return new Key<>(name, 1);
        }

        /** Create a new Key with given name an class. This <i>must</i> be used for isolated classes, such as plugins */
        public static <T> Key<T> named(String name, Class<?> owner) {
            return new Key<>(name, owner);
        }

        private static Class<?> lookupOwner(int stackFramesToOwner) {
            try {
                return Class.forName(Thread.currentThread().getStackTrace()[stackFramesToOwner + 2].getClassName());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        private Key(String name, int stackFramesToOwner) {
            this(name, lookupOwner(stackFramesToOwner + 1));
        }

        private Key(String name, Class owner) {
            this.name = String.format("%s<%s>", owner.getSimpleName(), name);
            this.owner = owner;
        }

        @Override
        public String toString() {
            return name;
        }

        Class<?> getOwner() {
            return owner;
        }
    }

    public static final class MapKey<K,V> extends Key<Map<K,V>> {

        public static <K,V> MapKey<K,V> mapNamed(String name) {
            return new MapKey<>(name);
        }

        private MapKey(String name) {
            super(name, 3);
        }

        Key<Map<K,V>> asKey() {
            return this;
        }
    }

    public static final class StackKey<T> extends Key<Deque<T>> {

        public static <K> StackKey<K> stackNamed(String name) {
            return new StackKey<>(name);
        }

        private StackKey(String name) {
            super(name, 3);
        }

        public Key<Deque<T>> asKey() {
            return this;
        }
    }
}
