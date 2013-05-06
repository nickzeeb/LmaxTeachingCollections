package com.lmax.collections.coalescing.ring.buffer;

import java.util.*;

// Performance = 20 kOPS
public class CoalescingSynchronizedBuffer<K, V> implements CoalescingBuffer<K, V> {
    private final int capacity;
    private final LinkedHashMap<K, V> linkedHashMap;

    public CoalescingSynchronizedBuffer(int capacity) {
        this.capacity = capacity;
        this.linkedHashMap = new LinkedHashMap<K, V>(capacity);
    }

    @Override public synchronized boolean offer(K key, V value) {
        if (linkedHashMap.containsKey(key)) {
            linkedHashMap.put(key, value);
            return true;
        }

        if (linkedHashMap.size() == capacity) {
            return false;
        }

        linkedHashMap.put(key, value);
        return true;
    }

    @Override public synchronized int poll(Collection<? super V> bucket) {
        int size = linkedHashMap.size();

        for (V value : linkedHashMap.values()) {
            bucket.add(value);
        }

        linkedHashMap.clear();
        return size;
    }

}