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

import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestMysqlEventListenerConfig
{
    @Test
    public void testDefaults()
            throws Exception
    {
        assertRecordedDefaults(recordDefaults(MysqlEventListenerConfig.class)
                .setConnectionUrl("jdbc:mysql://localhost:3306/db_name")
                .setConnectionUser("user")
                .setConnectionPassword("password"));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = Map.of(
                "mysql-event-listener.connection-url", "jdbc:mysql://localhost:3306/tahoe",
                "mysql-event-listener.connection-user", "tahoe",
                "mysql-event-listener.connection-password", "tahoe");

        MysqlEventListenerConfig expected = new MysqlEventListenerConfig()
                .setConnectionUrl("jdbc:mysql://localhost:3306/tahoe")
                .setConnectionUser("tahoe")
                .setConnectionPassword("tahoe");

        assertFullMapping(properties, expected);
    }
}
