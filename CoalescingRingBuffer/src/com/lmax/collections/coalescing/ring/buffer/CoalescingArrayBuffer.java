/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lmax.collections.coalescing.ring.buffer;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReferenceArray;

public final class CoalescingArrayBuffer<K, V> implements CoalescingBuffer<K, V> {

    private volatile long nextWrite = 1; // the next write index
    private final K[] keys;
    private final AtomicReferenceArray<V> atomicReferenceArray;

    @SuppressWarnings("unchecked")
    private final K nonCollapsibleKey = (K) new Object();
    private final int capacity;

    private volatile long firstWrite = 1; // the oldest slot that is is safe to write to
    private volatile long lastRead = 0; // the newest slot that it is safe to overwrite

    @SuppressWarnings("unchecked")
    public CoalescingArrayBuffer(int capacity) {
        this.capacity = capacity;
        this.keys = (K[]) new Object[this.capacity];
        this.atomicReferenceArray = new AtomicReferenceArray<V>(this.capacity);
    }

    @Override
    public int size() {
        return (int) (nextWrite - lastRead - 1);
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public boolean isEmpty() {
        return firstWrite == nextWrite;
    }

    @Override
    public boolean isFull() {
        return size() == capacity;
    }

    @Override public boolean offer(K key, V value) {
        long nextWrite = this.nextWrite;

        for (long updatePosition = firstWrite; updatePosition < nextWrite; updatePosition++) {
            int index = computeIndex(updatePosition);

            if(key.equals(keys[index])) {
                atomicReferenceArray.set(index, value);

                if (updatePosition >= firstWrite) {  // check that the reader has not read it yet
                    return true;
                } else {
                    break;
                }
            }
        }

        return add(key, value);
    }

    @Override public int poll(Collection<? super V> bucket) {
        long lastRead = this.lastRead;
        long nextWrite = this.nextWrite;
        firstWrite = nextWrite;

        for (long readIndex = lastRead + 1; readIndex < nextWrite; readIndex++) {
            int index = computeIndex(readIndex);
            keys[index] = null;
            bucket.add(atomicReferenceArray.getAndSet(index, null));
        }

        this.lastRead = nextWrite - 1;
        return (int) (nextWrite - lastRead - 1);
    }

    private int computeIndex(long value) {
        return ((int) value) % capacity;
    }

    @Override
    public boolean offer(V value) {
        return add(nonCollapsibleKey, value);
    }

    private boolean add(K key, V value) {
        if (isFull()) {
            return false;
        }

        long nextWrite = this.nextWrite;
        int index = computeIndex(nextWrite);

        keys[index] = key;
        atomicReferenceArray.set(index, value);

        this.nextWrite = nextWrite + 1;
        return true;
    }

}

/*

    [exec] time 20.0s
    [exec] compression ratio = 30.0
    [exec] mops = 25.0

 */