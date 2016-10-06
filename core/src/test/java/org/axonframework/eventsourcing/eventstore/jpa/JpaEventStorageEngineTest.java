/*
 * Copyright (c) 2010-2013. Axon Framework
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

package org.axonframework.eventsourcing.eventstore.jpa;

import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.jpa.SimpleEntityManagerProvider;
import org.axonframework.common.transaction.NoTransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.*;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.UnknownSerializedTypeException;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.upcasting.event.NoOpEventUpcasterChain;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import java.sql.SQLException;

import static junit.framework.TestCase.assertEquals;
import static org.axonframework.eventsourcing.eventstore.EventStoreTestUtils.*;
import static org.axonframework.eventsourcing.eventstore.EventUtils.asStream;
import static org.junit.Assert.assertFalse;

/**
 * @author Rene de Waele
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/META-INF/spring/db-context.xml")
@Transactional
public class JpaEventStorageEngineTest extends BatchingEventStorageEngineTest {

    private JpaEventStorageEngine testSubject;

    @PersistenceContext
    private EntityManager entityManager;

    private EntityManagerProvider entityManagerProvider;

    @Autowired
    private DataSource dataSource;

    private PersistenceExceptionResolver defaultPersistenceExceptionResolver;

    @Before
    public void setUp() throws SQLException {
        entityManagerProvider = new SimpleEntityManagerProvider(entityManager);
        defaultPersistenceExceptionResolver = new SQLErrorCodesResolver(dataSource);
        setTestSubject(
                testSubject = createEngine(NoOpEventUpcasterChain.INSTANCE, defaultPersistenceExceptionResolver));
    }

    @Test
    public void testStoreAndLoadEventsFromDatastore() {
        testSubject.appendEvents(createEvents(2));
        entityManager.clear();
        assertEquals(2, asStream(testSubject.readEvents(AGGREGATE)).count());
    }

    @Test
    public void testStoreTwoExactSameSnapshots() {
        testSubject.storeSnapshot(createEvent(1));
        entityManager.clear();
        testSubject.storeSnapshot(createEvent(1));
    }

    @Test(expected = UnknownSerializedTypeException.class)
    public void testUnknownSerializedTypeCausesException() {
        testSubject.appendEvents(createEvent());
        entityManager.createQuery("UPDATE DomainEventEntry e SET e.payloadType = :type").setParameter("type", "unknown")
                .executeUpdate();
        testSubject.readEvents(AGGREGATE).peek();
    }

    @Test
    @SuppressWarnings({"JpaQlInspection", "OptionalGetWithoutIsPresent"})
    @DirtiesContext
    public void testStoreEventsWithCustomEntity() throws Exception {
        testSubject = new JpaEventStorageEngine(new XStreamSerializer(), NoOpEventUpcasterChain.INSTANCE,
                                                defaultPersistenceExceptionResolver, NoTransactionManager.INSTANCE, 100,
                                                entityManagerProvider, DirectEventSequencer.INSTANCE) {

            @Override
            protected EventData<?> createEventEntity(EventMessage<?> eventMessage, Serializer serializer) {
                return new CustomDomainEventEntry((DomainEventMessage<?>) eventMessage, serializer);
            }

            @Override
            protected DomainEventData<?> createSnapshotEntity(DomainEventMessage<?> snapshot, Serializer serializer) {
                return new CustomSnapshotEventEntry(snapshot, serializer);
            }

            @Override
            protected String domainEventEntryEntityName() {
                return CustomDomainEventEntry.class.getSimpleName();
            }

            @Override
            protected String snapshotEventEntryEntityName() {
                return CustomSnapshotEventEntry.class.getSimpleName();
            }
        };

        testSubject.appendEvents(createEvent(AGGREGATE, 1, "Payload1"));
        testSubject.storeSnapshot(createEvent(AGGREGATE, 1, "Snapshot1"));
        entityManager.clear();

        assertFalse(entityManager.createQuery("SELECT e FROM CustomDomainEventEntry e").getResultList().isEmpty());
        assertEquals("Snapshot1", testSubject.readSnapshot(AGGREGATE).get().getPayload());
        assertEquals("Payload1", testSubject.readEvents(AGGREGATE).peek().getPayload());
    }

    @Override
    protected AbstractEventStorageEngine createEngine(EventUpcasterChain upcasterChain) {
        return createEngine(upcasterChain, defaultPersistenceExceptionResolver);
    }

    @Override
    protected AbstractEventStorageEngine createEngine(PersistenceExceptionResolver persistenceExceptionResolver) {
        return createEngine(NoOpEventUpcasterChain.INSTANCE, persistenceExceptionResolver);
    }

    protected JpaEventStorageEngine createEngine(EventUpcasterChain upcasterChain,
                                                 PersistenceExceptionResolver persistenceExceptionResolver) {
        return new JpaEventStorageEngine(new XStreamSerializer(), upcasterChain, persistenceExceptionResolver,
                                         NoTransactionManager.INSTANCE, 100, entityManagerProvider,
                                         DirectEventSequencer.INSTANCE);
    }
}
