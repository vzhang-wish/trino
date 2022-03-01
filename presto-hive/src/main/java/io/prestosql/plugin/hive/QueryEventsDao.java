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

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface QueryEventsDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS presto_query_create_event(\n" +
            "metadata VARCHAR(4096))")
    void createQueryCreateEventTable();

    @SqlUpdate("INSERT INTO presto_query_create_event SET\n" +
            "metadata = :metadata")
    void insertQueryCreateEvent(
            @Bind String metadata);
}
