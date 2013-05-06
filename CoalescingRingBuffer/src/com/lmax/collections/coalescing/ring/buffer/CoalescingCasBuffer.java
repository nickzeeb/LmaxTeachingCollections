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
            LinkedHashMap<K, V> reference = atomicReference.get();
            LinkedHashMap<K, V> map = (LinkedHashMap<K, V>) reference.clone();

            if (map.containsKey(key)) {
                map.put(key, value);
            }
            else if (map.size() == capacity) {
                return false;
            }
            else {
                map.put(key, value);
            }

            success = atomicReference.compareAndSet(reference, map);
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