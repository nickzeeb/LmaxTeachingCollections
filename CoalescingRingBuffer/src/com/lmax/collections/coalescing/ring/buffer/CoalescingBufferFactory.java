package com.lmax.collections.coalescing.ring.buffer;

public class CoalescingBufferFactory {

    public static <K,V> CoalescingBuffer<K,V> create(int capacity) {
        return new CoalescingSingleWriterBuffer<K, V>(capacity);
    }

}