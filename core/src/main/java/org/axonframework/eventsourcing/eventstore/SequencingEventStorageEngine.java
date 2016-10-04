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

import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.common.transaction.Transaction;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.upcasting.event.NoOpEventUpcasterChain;
import org.axonframework.serialization.xml.XStreamSerializer;

import java.util.Iterator;
import java.util.List;

import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * @author Rene de Waele
 */
public abstract class SequencingEventStorageEngine extends BatchingEventStorageEngine {

    private final EventSequencer eventSequencer;
    private final TransactionManager transactionManager;

    /**
     * Initializes an EventStorageEngine with given {@code serializer}, {@code upcasterChain} and {@code
     * persistenceExceptionResolver}.
     *
     * @param serializer                   Used to serialize and deserialize event payload and metadata. If {@code null}
     *                                     an {@link XStreamSerializer} is used.
     * @param upcasterChain                Allows older revisions of serialized objects to be deserialized. If {@code
     *                                     null} a {@link NoOpEventUpcasterChain} is used.
     * @param persistenceExceptionResolver Detects concurrency exceptions from the backing database. If {@code null}
     *                                     persistence exceptions are not explicitly resolved.
     * @param batchSize                    The number of events that should be read at each database access. When more
     *                                     than this number of events must be read to rebuild an aggregate's state, the
     *                                     events are read in batches of this size. If {@code null} a batch size of 100
     *                                     is used. Tip: if you use a snapshotter, make sure to choose snapshot trigger
     *                                     and batch
     * @param eventSequencer               Creates tracking tokens for events after they are appended to the event
     *                                     store. If {@code null} a {@link DefaultEventSequencer} is used.
     * @param transactionManager           Manages transactions in which the stored events are sequenced (receive a
     *                                     unique tracking token)
     */
    public SequencingEventStorageEngine(Serializer serializer, EventUpcasterChain upcasterChain,
                                        PersistenceExceptionResolver persistenceExceptionResolver, Integer batchSize,
                                        EventSequencer eventSequencer, TransactionManager transactionManager) {
        super(serializer, upcasterChain, persistenceExceptionResolver, batchSize);
        this.eventSequencer = getOrDefault(eventSequencer, new DefaultEventSequencer());
        this.transactionManager = transactionManager;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method defers to {@link #doAppendEvents(List, Serializer)} to actually append the events. After appending,
     * the tracking token of the stored events is updated using the embedded {@link EventSequencer}.
     */
    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        if (events.isEmpty()) {
            return;
        }
        try {
            doAppendEvents(events, serializer);
            eventSequencer.updateSequence(uow -> supplementMissingTrackingTokens());
        } catch (Exception e) {
            handlePersistenceException(e, events.get(0));
        }
    }

    /**
     * Append given {@code events} to the backing database. Use the given {@code serializer} to serialize the event's
     * payload and metadata.
     *
     * @param events     Events to append to the database
     * @param serializer Serializer used to convert the events to a suitable format for storage
     */
    protected abstract void doAppendEvents(List<? extends EventMessage<?>> events, Serializer serializer);

    /**
     * Supplements missing tracking tokens to events in the store. If there are no events without tracking token this
     * method returns {@code false}, otherwise {@code true} is returned.
     *
     * @return {@code false} if the store does not contain any events with missing tracking tokens, {@code true}
     * otherwise
     */
    protected boolean supplementMissingTrackingTokens() {
        List<?> unsequencedEventIds = getUnsequencedEventIds();
        if (unsequencedEventIds.isEmpty()) {
            return false;
        }
        Iterator<TrackingToken> trackingTokenSupplier = trackingTokenSupplier();
        Transaction transaction = transactionManager.startTransaction();
        try {
            for (Object eventId : unsequencedEventIds) {
                updateTrackingToken(eventId, trackingTokenSupplier.next());
            }
        } catch (Exception e) {
            if (persistenceExceptionResolver().isDuplicateKeyViolation(e)) {
                transaction.rollback();
                return true;
            }
            throw e;
        }
        transaction.commit();
        return true;
    }

    /**
     * Update the event entry with given {@code eventId} by giving it the given {@code nextTrackingToken}.
     *
     * @param eventId           The id of the event to update
     * @param nextTrackingToken The tracking token to give the event
     */
    protected abstract void updateTrackingToken(Object eventId, TrackingToken nextTrackingToken);

    /**
     * Returns a supplier of tracking tokens. Each time the supplier is asked to produce the next token it supplies the
     * next available tracking token in the store. Once the first token is supplied the supplier may choose to query
     * the event storage for the next available token or alternatively derive the next token without querying the store.
     *
     * @return supplier of the next available tracking token in the store
     */
    protected abstract Iterator<TrackingToken> trackingTokenSupplier();

    /**
     * Returns a list of primary keys for all events that do not have a tracking token value yet. The order of keys in
     * the returned list should follow the insertion order of the events.
     *
     * @return list of primary keys of events without global tracking token
     */
    protected abstract List<?> getUnsequencedEventIds();
}
