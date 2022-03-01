/*
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
package io.prestosql.plugin.hive;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.concurrent.Executor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class MysqlEventListener
        implements EventListener
{
    private final Logger log = Logger.get(MysqlEventListener.class);

    private final Executor executor = new BoundedExecutor(newCachedThreadPool(daemonThreadsNamed("mysql-event-writer-%s")), 10);
    private final JsonCodec<QueryCompletedEvent> queryCompletedEventJsonCodec;
    private final JsonCodec<QueryCreatedEvent> queryCreatedEventJsonCodec;
    private final QueryEventsDao queryEventsDao;

    @Inject
    public MysqlEventListener(
            JsonCodec<QueryCompletedEvent> queryCompletedEventJsonCodec,
            JsonCodec<QueryCreatedEvent> queryCreatedEventJsonCodec,
            MysqlEventListenerConfig config)
    {
        this.queryCompletedEventJsonCodec = requireNonNull(queryCompletedEventJsonCodec, "queryCompletedEventJsonCodec is null");
        this.queryCreatedEventJsonCodec = requireNonNull(queryCreatedEventJsonCodec, "queryCreatedEventJsonCodec is null");
        Jdbi jdbi = Jdbi.create(config.getConnectionUrl(), config.getConnectionUser(), config.getConnectionPassword());
        jdbi.installPlugin(new SqlObjectPlugin());
        queryEventsDao = jdbi.onDemand(QueryEventsDao.class);
        queryEventsDao.createQueryCreateEventTable();
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        log.info("query created 2022 test: " + queryCreatedEventJsonCodec.toJson(queryCreatedEvent));
        executor.execute(() -> {
            try {
                queryEventsDao.insertQueryCreateEvent(queryCreatedEventJsonCodec.toJson(queryCreatedEvent));
            }
            catch (RuntimeException e) {
                log.warn(format("Failed to log query create event from user %s to mysql", queryCreatedEvent.getContext().getUser()));
            }
        });
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        //log.info("query completed 2022: " + queryCompletedEventJsonCodec.toJson(queryCompletedEvent));
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
