/*
 * Copyright (c) 2010-2016. Axon Framework
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.eventsourcing.eventstore;

import java.util.Objects;

/**
 * Default implementation of a {@link TrackingToken} that uses the global commit sequence number of the event to
 * determine tracking order.
 *
 * @author Rene de Waele
 */
public class DefaultTrackingToken implements TrackingToken {

    private final long index;

    public DefaultTrackingToken(long index) {
        this.index = index;
    }

    @Override
    public boolean isGuaranteedNext(TrackingToken otherToken) {
        return ((DefaultTrackingToken) otherToken).index - index == 1;
    }

    public long getIndex() {
        return index;
    }

    public DefaultTrackingToken offsetBy(int offset) {
        return new DefaultTrackingToken(index + offset);
    }

    public DefaultTrackingToken next() {
        return new DefaultTrackingToken(index + 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultTrackingToken that = (DefaultTrackingToken) o;
        return index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    @Override
    public int compareTo(TrackingToken o) {
        return Long.compare(index, ((DefaultTrackingToken) o).index);
    }

    @Override
    public String toString() {
        return "DefaultTrackingToken{" + "index=" + index + '}';
    }
}
