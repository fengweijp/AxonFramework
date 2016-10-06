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

package org.axonframework.eventsourcing.eventstore.jdbc;

import org.axonframework.commandhandling.model.ConcurrencyException;
import org.axonframework.common.Assert;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.*;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;

/**
 * EventStorageEngine implementation that uses JDBC to store and fetch events.
 * <p>
 * By default the payload of events is stored as a serialized blob of bytes. Other columns are used to store meta-data
 * that allow quick finding of DomainEvents for a specific aggregate in the correct order.
 *
 * @author Rene de Waele
 */
public class JdbcEventStorageEngine extends AbstractJdbcEventStorageEngine {

    private static final Random random = new Random();

    private final Class<?> dataType;
    private final EventSchema schema;

    /**
     * Initializes an EventStorageEngine that uses JDBC to store and load events using the default {@link EventSchema}.
     * The payload and metadata of events is stored as a serialized blob of bytes using the given {@code serializer}.
     * <p>
     * Events are read in batches of 100. The given {@code upcasterChain} is used to upcast events before
     * deserialization.
     * <p>
     * A {@link DefaultEventSequencer} is used to supply even entries with tracking tokens.
     *
     * @param serializer                   Used to serialize and deserialize event payload and metadata.
     * @param upcasterChain                Allows older revisions of serialized objects to be deserialized.
     * @param persistenceExceptionResolver Detects concurrency exceptions from the backing database.
     * @param connectionProvider           The provider of connections to the underlying database
     * @param transactionManager           The transaction manager used to set the isolation level of the transaction
     *                                     when loading events
     */
    public JdbcEventStorageEngine(Serializer serializer, EventUpcasterChain upcasterChain,
                                  PersistenceExceptionResolver persistenceExceptionResolver,
                                  TransactionManager transactionManager, ConnectionProvider connectionProvider) {
        this(serializer, upcasterChain, persistenceExceptionResolver, transactionManager, null, connectionProvider,
             byte[].class, new EventSchema(), null);
    }

    /**
     * Initializes an EventStorageEngine that uses JDBC to store and load events using the default {@link EventSchema}.
     * The payload and metadata of events is stored as a serialized blob of bytes using the given {@code serializer}.
     * <p>
     * Events are read in batches of 100. The given {@code upcasterChain} is used to upcast events before
     * deserialization.
     *
     * @param serializer                   Used to serialize and deserialize event payload and metadata.
     * @param upcasterChain                Allows older revisions of serialized objects to be deserialized.
     * @param persistenceExceptionResolver Detects concurrency exceptions from the backing database.
     * @param connectionProvider           The provider of connections to the underlying database
     * @param transactionManager           The transaction manager used to set the isolation level of the transaction
     *                                     when loading events
     * @param eventSequencer               Creates tracking tokens for events after they are appended to the event
     *                                     store.
     */
    public JdbcEventStorageEngine(Serializer serializer, EventUpcasterChain upcasterChain,
                                  PersistenceExceptionResolver persistenceExceptionResolver,
                                  TransactionManager transactionManager, ConnectionProvider connectionProvider,
                                  EventSequencer eventSequencer) {
        this(serializer, upcasterChain, persistenceExceptionResolver, transactionManager, null, connectionProvider,
             byte[].class, new EventSchema(), eventSequencer);
    }

    /**
     * Initializes an EventStorageEngine that uses JDBC to store and load events.
     *
     * @param serializer                   Used to serialize and deserialize event payload and metadata.
     * @param upcasterChain                Allows older revisions of serialized objects to be deserialized.
     * @param persistenceExceptionResolver Detects concurrency exceptions from the backing database.
     * @param transactionManager           The transaction manager used to set the isolation level of the transaction
     *                                     when loading events
     * @param batchSize                    The number of events that should be read at each database access. When more
     *                                     than this number of events must be read to rebuild an aggregate's state, the
     *                                     events are read in batches of this size. Tip: if you use a snapshotter, make
     *                                     sure to choose snapshot trigger and batch size such that a single batch will
     *                                     generally retrieve all events required to rebuild an aggregate's state.
     * @param connectionProvider           The provider of connections to the underlying database
     * @param dataType                     The data type for serialized event payload and metadata
     * @param schema                       Object that describes the database schema of event entries
     * @param eventSequencer               Creates tracking tokens for events after they are appended to the event
     *                                     store.
     */
    public JdbcEventStorageEngine(Serializer serializer, EventUpcasterChain upcasterChain,
                                  PersistenceExceptionResolver persistenceExceptionResolver,
                                  TransactionManager transactionManager, Integer batchSize,
                                  ConnectionProvider connectionProvider, Class<?> dataType, EventSchema schema,
                                  EventSequencer eventSequencer) {
        super(serializer, upcasterChain, persistenceExceptionResolver, transactionManager, batchSize,
              connectionProvider, eventSequencer);
        this.dataType = dataType;
        this.schema = schema;
    }

    @Override
    public void createSchema(EventTableFactory schemaFactory) {
        executeUpdates(e -> {
                           throw new EventStoreException("Failed to create event tables", e);
                       }, connection -> schemaFactory.createDomainEventTable(connection, schema),
                       connection -> schemaFactory.createSnapshotEventTable(connection, schema));
    }

    @Override
    public PreparedStatement appendEvent(Connection connection, DomainEventMessage<?> event,
                                         Serializer serializer) throws SQLException {
        SerializedObject<?> payload = serializer.serialize(event.getPayload(), dataType);
        SerializedObject<?> metaData = serializer.serialize(event.getMetaData(), dataType);
        final String sql = "INSERT INTO " + schema.domainEventTable() + " (" +
                String.join(", ", schema.trackingTokenColumn(), schema.eventIdentifierColumn(),
                            schema.aggregateIdentifierColumn(), schema.sequenceNumberColumn(), schema.typeColumn(),
                            schema.timestampColumn(), schema.payloadTypeColumn(), schema.payloadRevisionColumn(),
                            schema.payloadColumn(), schema.metaDataColumn()) + ") VALUES (?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql); // NOSONAR
        preparedStatement.setLong(1, Math.negateExact(random.nextInt(Integer.MAX_VALUE)));
        preparedStatement.setString(2, event.getIdentifier());
        preparedStatement.setString(3, event.getAggregateIdentifier());
        preparedStatement.setLong(4, event.getSequenceNumber());
        preparedStatement.setString(5, event.getType());
        writeTimestamp(preparedStatement, 6, event.getTimestamp());
        preparedStatement.setString(7, payload.getType().getName());
        preparedStatement.setString(8, payload.getType().getRevision());
        preparedStatement.setObject(9, payload.getData());
        preparedStatement.setObject(10, metaData.getData());
        return preparedStatement;
    }

    @Override
    public PreparedStatement appendSnapshot(Connection connection, DomainEventMessage<?> snapshot,
                                            Serializer serializer) throws SQLException {
        SerializedObject<?> payload = serializer.serialize(snapshot.getPayload(), dataType);
        SerializedObject<?> metaData = serializer.serialize(snapshot.getMetaData(), dataType);
        final String sql = "INSERT INTO " + schema.snapshotTable() + " (" +
                String.join(", ", schema.eventIdentifierColumn(), schema.aggregateIdentifierColumn(),
                            schema.sequenceNumberColumn(), schema.typeColumn(), schema.timestampColumn(),
                            schema.payloadTypeColumn(), schema.payloadRevisionColumn(), schema.payloadColumn(),
                            schema.metaDataColumn()) + ") VALUES (?,?,?,?,?,?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql); // NOSONAR
        preparedStatement.setString(1, snapshot.getIdentifier());
        preparedStatement.setString(2, snapshot.getAggregateIdentifier());
        preparedStatement.setLong(3, snapshot.getSequenceNumber());
        preparedStatement.setString(4, snapshot.getType());
        writeTimestamp(preparedStatement, 5, snapshot.getTimestamp());
        preparedStatement.setString(6, payload.getType().getName());
        preparedStatement.setString(7, payload.getType().getRevision());
        preparedStatement.setObject(8, payload.getData());
        preparedStatement.setObject(9, metaData.getData());
        return preparedStatement;
    }

    @Override
    protected List<?> getUnsequencedEventIds() {
        return executeQuery(connection -> {
            final String sql =
                    "SELECT " + schema.globalIndexColumn() + " FROM " + schema.domainEventTable() + " WHERE " +
                            schema.trackingTokenColumn() + " < 0 ORDER BY " + schema.globalIndexColumn() + " ASC";
            return connection.prepareStatement(sql);
        }, resultSet -> resultSet.getLong(1), e -> new EventStoreException(
                "Failed to get identifiers of event entries that are missing a tracking token", e));
    }

    @Override
    protected Supplier<TrackingToken> trackingTokenSupplier() {
        Optional<Long> highestToken = executeQuery(connection -> {
            final String sql =
                    "SELECT MAX(" + schema.trackingTokenColumn() + ") FROM " + schema.domainEventTable() + " WHERE " +
                            schema.trackingTokenColumn() + " >= 0";
            return connection.prepareStatement(sql);
        }, resultSet -> resultSet.getLong(1), e -> new EventStoreException(
                "Failed to get the highest tracking token of stored event entries", e)).stream()
                .filter(Objects::nonNull).findFirst();
        DefaultTrackingToken first = highestToken.map(DefaultTrackingToken::new).map(DefaultTrackingToken::next)
                .orElse(new DefaultTrackingToken(0L));
        return new Supplier<TrackingToken>() {
            private DefaultTrackingToken last;

            @Override
            public TrackingToken get() {
                return last = (last == null ? first : last.next());
            }
        };
    }

    @Override
    protected void updateTrackingToken(Object eventId, TrackingToken nextTrackingToken) {
        executeUpdates(e -> {
            if (persistenceExceptionResolver() != null && persistenceExceptionResolver().isDuplicateKeyViolation(e)) {
                throw new ConcurrencyException(String.format(
                        "Failed to update event [%s] with tracking token [%s]. " + "Tracking token is already in use.",
                        eventId, nextTrackingToken), e);
            }
            throw new EventStoreException(
                    String.format("Failed to update event [%s] with tracking token [%s].", eventId, nextTrackingToken),
                    e);
        }, connection -> {
            String sql =
                    "UPDATE " + schema.domainEventTable() + " SET " + schema.trackingTokenColumn() + " = ? WHERE " +
                            schema.globalIndexColumn() + " = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setLong(1, ((DefaultTrackingToken) nextTrackingToken).getIndex());
            preparedStatement.setLong(2, (Long) eventId);
            return preparedStatement;
        });
    }

    @Override
    public PreparedStatement deleteSnapshots(Connection connection, String aggregateIdentifier) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "DELETE FROM " + schema.snapshotTable() + " WHERE " + schema.aggregateIdentifierColumn() + " = ?");
        preparedStatement.setString(1, aggregateIdentifier);
        return preparedStatement;
    }

    @Override
    public PreparedStatement readEventData(Connection connection, String identifier,
                                           long firstSequenceNumber) throws SQLException {
        final String sql = "SELECT " + trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE " +
                schema.aggregateIdentifierColumn() + " = ? AND " + schema.sequenceNumberColumn() + " >= ? ORDER BY " +
                schema.sequenceNumberColumn() + " ASC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, identifier);
        preparedStatement.setLong(2, firstSequenceNumber);
        return preparedStatement;
    }

    @Override
    public PreparedStatement readEventData(Connection connection, TrackingToken lastToken) throws SQLException {
        Assert.isTrue(lastToken == null || lastToken instanceof DefaultTrackingToken,
                      String.format("Token [%s] is of the wrong type", lastToken));
        final String sql = "SELECT " + trackedEventFields() + " FROM " + schema.domainEventTable() + " WHERE " +
                schema.trackingTokenColumn() + " > ? ORDER BY " + schema.trackingTokenColumn() + " ASC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, lastToken == null ? -1 : ((DefaultTrackingToken) lastToken).getIndex());
        return preparedStatement;
    }

    @Override
    public PreparedStatement readSnapshotData(Connection connection, String identifier) throws SQLException {
        final String s = "SELECT " + domainEventFields() + " FROM " + schema.snapshotTable() + " WHERE " +
                schema.aggregateIdentifierColumn() + " = ? ORDER BY " + schema.sequenceNumberColumn() + " DESC";
        PreparedStatement statement = connection.prepareStatement(s);
        statement.setString(1, identifier);
        return statement;
    }

    @Override
    public TrackedEventData<?> getTrackedEventData(ResultSet resultSet) throws SQLException {
        return new GenericTrackedDomainEventEntry<>(resultSet.getLong(schema.trackingTokenColumn()),
                                                    resultSet.getString(schema.typeColumn()),
                                                    resultSet.getString(schema.aggregateIdentifierColumn()),
                                                    resultSet.getLong(schema.sequenceNumberColumn()),
                                                    resultSet.getString(schema.eventIdentifierColumn()),
                                                    readTimeStamp(resultSet, schema.timestampColumn()),
                                                    resultSet.getString(schema.payloadTypeColumn()),
                                                    resultSet.getString(schema.payloadRevisionColumn()),
                                                    readPayload(resultSet, schema.payloadColumn()),
                                                    readPayload(resultSet, schema.metaDataColumn()));
    }

    @Override
    public DomainEventData<?> getDomainEventData(ResultSet resultSet) throws SQLException {
        return (DomainEventData<?>) getTrackedEventData(resultSet);
    }

    @Override
    protected DomainEventData<?> getSnapshotData(ResultSet resultSet) throws SQLException {
        return new GenericDomainEventEntry<>(resultSet.getString(schema.typeColumn()),
                                             resultSet.getString(schema.aggregateIdentifierColumn()),
                                             resultSet.getLong(schema.sequenceNumberColumn()),
                                             resultSet.getString(schema.eventIdentifierColumn()),
                                             readTimeStamp(resultSet, schema.timestampColumn()),
                                             resultSet.getString(schema.payloadTypeColumn()),
                                             resultSet.getString(schema.payloadRevisionColumn()),
                                             readPayload(resultSet, schema.payloadColumn()),
                                             readPayload(resultSet, schema.metaDataColumn()));
    }

    /**
     * Reads a timestamp from the given {@code resultSet} at given {@code columnIndex}. The resultSet is
     * positioned in the row that contains the data. This method must not change the row in the result set.
     *
     * @param resultSet  The resultSet containing the stored data
     * @param columnName The name of the column containing the timestamp
     * @return an object describing the timestamp
     * @throws SQLException when an exception occurs reading from the resultSet.
     */
    protected Object readTimeStamp(ResultSet resultSet, String columnName) throws SQLException {
        return resultSet.getString(columnName);
    }

    /**
     * Write a timestamp from a {@link TemporalAccessor} to a data value suitable for the database scheme.
     *
     * @param input {@link TemporalAccessor} to convert
     */
    protected void writeTimestamp(PreparedStatement preparedStatement, int position,
                                  TemporalAccessor input) throws SQLException {
        preparedStatement.setString(position, Instant.from(input).toString());
    }

    /**
     * Reads a serialized object from the given {@code resultSet} at given {@code columnIndex}. The resultSet
     * is positioned in the row that contains the data. This method must not change the row in the result set.
     *
     * @param resultSet  The resultSet containing the stored data
     * @param columnName The name of the column containing the payload
     * @return an object describing the serialized data
     * @throws SQLException when an exception occurs reading from the resultSet.
     */
    @SuppressWarnings("unchecked")
    protected <T> T readPayload(ResultSet resultSet, String columnName) throws SQLException {
        if (byte[].class.equals(dataType)) {
            return (T) resultSet.getBytes(columnName);
        }
        return (T) resultSet.getObject(columnName);
    }

    protected String domainEventFields() {
        return String.join(", ", schema.eventIdentifierColumn(), schema.timestampColumn(), schema.payloadTypeColumn(),
                           schema.payloadRevisionColumn(), schema.payloadColumn(), schema.metaDataColumn(),
                           schema.typeColumn(), schema.aggregateIdentifierColumn(), schema.sequenceNumberColumn());
    }

    protected String trackedEventFields() {
        return schema.trackingTokenColumn() + ", " + domainEventFields();
    }

    protected EventSchema schema() {
        return schema;
    }

    protected Class<?> dataType() {
        return dataType;
    }
}
