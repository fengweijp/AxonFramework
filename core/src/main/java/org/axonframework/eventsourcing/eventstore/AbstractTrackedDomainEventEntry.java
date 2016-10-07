/*
 * Copyright (c) 2010-2016. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.eventsourcing.eventstore;

import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.serialization.Serializer;

import javax.persistence.*;
import java.util.Random;

/**
 * Abstract base class of a serialized domain event that can be used by tracking event stores. Fields in this class
 * contain JPA annotations that direct JPA event storage engines how to store event entries.
 *
 * @author Rene de Waele
 */
@MappedSuperclass
public abstract class AbstractTrackedDomainEventEntry<T> extends AbstractDomainEventEntry<T> implements TrackedEventData<T> {

    private static final Random random = new Random();

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @SuppressWarnings("unused")
    private long globalIndex;
    @Column(unique = true)
    private long trackingToken;

    /**
     * Construct a new event entry from a published domain event message to enable storing the event or sending it to a
     * remote location.
     * <p>
     * The given {@code serializer} will be used to serialize the payload and metadata in the given {@code
     * eventMessage}. The type of the serialized data will be the same as the given {@code contentType}.
     *
     * @param eventMessage The event message to convert to a serialized event entry
     * @param serializer   The serializer to convert the event
     * @param contentType  The data type of the payload and metadata after serialization
     */
    public AbstractTrackedDomainEventEntry(DomainEventMessage<?> eventMessage, Serializer serializer,
                                           Class<T> contentType) {
        super(eventMessage, serializer, contentType);
        trackingToken = -Math.abs(random.nextLong());
    }

    /**
     * Reconstruct an event entry from a stored object.
     *
     * @param type                The type of aggregate that published this event
     * @param aggregateIdentifier The identifier of the aggregate that published this event
     * @param sequenceNumber      The sequence number of the event in the aggregate
     * @param trackingToken       The global commit sequence number of the event. Can be used to track the event store.
     * @param eventIdentifier     The identifier of the event
     * @param timestamp           The time at which the event was originally created
     * @param payloadType         The fully qualified class name or alias of the event payload
     * @param payloadRevision     The revision of the event payload
     * @param payload             The serialized payload
     * @param metaData            The serialized metadata
     */
    public AbstractTrackedDomainEventEntry(long trackingToken, String eventIdentifier, Object timestamp,
                                           String payloadType, String payloadRevision, T payload, T metaData,
                                           String type, String aggregateIdentifier, long sequenceNumber) {
        super(type, aggregateIdentifier, sequenceNumber, eventIdentifier, timestamp, payloadType, payloadRevision,
              payload, metaData);
        this.trackingToken = trackingToken;
    }

    /**
     * Default constructor required by JPA
     */
    protected AbstractTrackedDomainEventEntry() {
    }

    @Override
    public TrackingToken trackingToken() {
        return new DefaultTrackingToken(trackingToken);
    }
}
