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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.lmax.collections.coalescing.ring.buffer.MarketSnapshot.createMarketSnapshot;
import static org.junit.Assert.*;
import static org.junit.Assert.assertSame;

public class FunctionalTest {
    private final static MarketSnapshot VOD_SNAPSHOT_1 = createMarketSnapshot(1, 3, 4);
    private final static MarketSnapshot VOD_SNAPSHOT_2 = createMarketSnapshot(1, 5, 6);
    private final static MarketSnapshot BP_SNAPSHOT = createMarketSnapshot(2, 7, 8);

    protected CoalescingBuffer<Long, MarketSnapshot> buffer;

    @Before
    public void beforeEveryTest() {
        buffer = createBuffer(10);
    }

    public CoalescingBuffer<Long, MarketSnapshot> createBuffer(int capacity) {
        return CoalescingBufferFactory.create(capacity);
    }

    @Test
    public void shouldRejectNewValueWhenFull() {
        buffer = createBuffer(2);
        buffer.offer(1L, BP_SNAPSHOT);
        buffer.offer(2L, VOD_SNAPSHOT_1);

        assertFalse(buffer.offer(4L, VOD_SNAPSHOT_2));
    }

    @Test
    public void shouldAcceptUpdatedValueWhenFull() {
        buffer = createBuffer(2);
        buffer.offer(1L, BP_SNAPSHOT);
        buffer.offer(2L, BP_SNAPSHOT);

        assertTrue(buffer.offer(2L, BP_SNAPSHOT));
    }

    @Test
    public void shouldReturnOneUpdate() {
        add(BP_SNAPSHOT);
        assertContains(BP_SNAPSHOT);
    }

    @Test
    public void shouldReturnTwoDifferentUpdates() {
        add(BP_SNAPSHOT);
        add(VOD_SNAPSHOT_1);

        assertContains(BP_SNAPSHOT, VOD_SNAPSHOT_1);
    }

    @Test
    public void shouldCollapseTwoUpdatesOnSameTopic() {
        add(VOD_SNAPSHOT_1);
        add(VOD_SNAPSHOT_2);

        assertContains(VOD_SNAPSHOT_2);
    }

    @Test
    public void shouldCollapseTwoUpdatesOnSameTopicAndPreserveOrdering() {
        add(VOD_SNAPSHOT_1);
        add(BP_SNAPSHOT);
        add(VOD_SNAPSHOT_2);

        assertContains(VOD_SNAPSHOT_2, BP_SNAPSHOT);
    }

    @Test
    public void shouldNotCollapseValuesIfReadFastEnough() {
        add(VOD_SNAPSHOT_1);
        assertContains(VOD_SNAPSHOT_1);

        add(VOD_SNAPSHOT_2);
        assertContains(VOD_SNAPSHOT_2);
    }

    @Test
    public void shouldUseObjectEqualityToCompareKeys() throws Exception {
        CoalescingBuffer<String, Object> buffer = CoalescingBufferFactory.create(2);

        buffer.offer(new String("foo"), new Object());
        buffer.offer(new String("boo"), new Object());
        assertTrue(buffer.offer(new String("boo"), new Object()));
    }

    private void add(MarketSnapshot snapshot) {
        assertTrue(buffer.offer(snapshot.getInstrumentId(), snapshot));
    }

    private void assertContains(MarketSnapshot... expected) {
        List<MarketSnapshot> actualSnapshots = new ArrayList<MarketSnapshot>(expected.length);

        int readCount = buffer.poll(actualSnapshots);
        assertSame(expected.length, readCount);

        for (int i = 0; i < expected.length; i++) {
            assertSame(expected[i], actualSnapshots.get(i));
        }

    }

}