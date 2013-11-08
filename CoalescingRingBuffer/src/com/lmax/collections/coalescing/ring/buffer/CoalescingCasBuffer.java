package com.lmax.collections.coalescing.ring.buffer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("unchecked")
// Performance = 0.23 kOPS
public class CoalescingCasBuffer<K,V> implements CoalescingBuffer<K,V> {
    private final int capacity;
    private final AtomicReference<LinkedHashMap<K,V>> atomicReference = new AtomicReference<LinkedHashMap<K, V>>();

    public CoalescingCasBuffer(int capacity) {
        this.capacity = capacity;
        atomicReference.set(new LinkedHashMap<K, V>(capacity));
    }

    @Override public boolean offer(K key, V value) {
        boolean success = false;

        while (!success) {
            LinkedHashMap<K, V> current = atomicReference.get();
            LinkedHashMap<K, V> modified = (LinkedHashMap<K, V>) current.clone();

            if (modified.containsKey(key)) {
                modified.put(key, value);
            }
            else if (modified.size() == capacity) {
                return false;
            }
            else {
                modified.put(key, value);
            }

            success = atomicReference.compareAndSet(current, modified);
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

}