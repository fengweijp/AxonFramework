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

import org.axonframework.common.AxonThreadFactory;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author Rene de Waele
 */
public class DefaultEventSequencer implements EventSequencer {

    private static final ThreadGroup THREAD_GROUP = new ThreadGroup(DefaultEventSequencer.class.getSimpleName());

    private final ThreadFactory threadFactory;
    private final AtomicBoolean updatingTrackingToken = new AtomicBoolean();
    private volatile boolean shuttingDown;

    public DefaultEventSequencer() {
        this(new AxonThreadFactory(THREAD_GROUP));
    }

    public DefaultEventSequencer(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    @Override
    public void updateSequence(Function<UnitOfWork<?>, Boolean> updateFunction) {
        if (!CurrentUnitOfWork.ifStarted(uow -> uow.afterCommit(u -> doUpdateSequence(u, updateFunction)))) {
            doUpdateSequence(null, updateFunction);
        }
    }

    protected void doUpdateSequence(UnitOfWork<?> unitOfWork, Function<UnitOfWork<?>, Boolean> updateFunction) {
        threadFactory.newThread(() -> {
            while (updatingTrackingToken.compareAndSet(false, true) && !shuttingDown) {
                try {
                    if (!updateFunction.apply(unitOfWork)) {
                        break;
                    }
                } finally {
                    updatingTrackingToken.set(false);
                }
            }
        }).start();
    }

    @Override
    public void shutDown() {
        shuttingDown = true;
    }
}
