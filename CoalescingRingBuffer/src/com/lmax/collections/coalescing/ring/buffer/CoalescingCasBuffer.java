package com.lmax.collections.coalescing.ring.buffer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class CoalescingCasBuffer<K,V> implements CoalescingBuffer<K,V> {
    private final int capacity;
    private final AtomicReference<LinkedHashMap<K,V>> atomicReference = new AtomicReference<LinkedHashMap<K, V>>();

    public CoalescingCasBuffer(int capacity) {
        this.capacity = capacity;
        atomicReference.set(new LinkedHashMap<K, V>(capacity));
    }

    @Override
    public int size() {
        return atomicReference.get().size();
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean isFull() {
        return size() == capacity;
    }

    @Override public boolean offer(K key, V value) {
        boolean success = false;

        while (!success) {
            LinkedHashMap<K, V> map = atomicReference.get();

            if (map.containsKey(key)) {
                map.put(key, value);
            }
            else if (map.size() == capacity) {
                return false;
            }
            else {
                map.put(key, value);
            }

            success = atomicReference.compareAndSet(map, map);
        }

        return true;
    }

    @Override public int poll(Collection<? super V> bucket) {
        LinkedHashMap<K, V> map = atomicReference.getAndSet(new LinkedHashMap<K, V>(capacity));

        for (V value : map.values()) {
            bucket.add(value);
        }

        return map.size();
    }

    @Override
    public boolean offer(V value) {
        return offer((K) new Object(), value);
    }

}

/**

 [exec] time 33.7s
 [exec] compression ratio = 1011.4
 [exec] mops = 14.7

 */
