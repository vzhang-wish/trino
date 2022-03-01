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

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.mysql.jdbc.Driver;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class MysqlEventListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "mysql";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                binder -> {
                    jsonCodecBinder(binder).bindJsonCodec(QueryCompletedEvent.class);
                    jsonCodecBinder(binder).bindJsonCodec(QueryCreatedEvent.class);
                    configBinder(binder).bindConfig(MysqlEventListenerConfig.class);
                    binder.bind(MysqlEventListener.class).in(Scopes.SINGLETON);
                    binder.bind(Driver.class).in(Scopes.SINGLETON);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(MysqlEventListener.class);
    }
}
