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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class MysqlEventListenerConfig
{
    private String connectionUrl = "jdbc:mysql://localhost:3306/db_name";
    private String connectionUser = "user";
    private String connectionPassword = "password";

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @ConfigDescription("mysql server url")
    @Config("mysql-event-listener.connection-url")
    public MysqlEventListenerConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @NotNull
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @ConfigDescription("user name for connection to mysql server")
    @Config("mysql-event-listener.connection-user")
    public MysqlEventListenerConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @NotNull
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @ConfigDescription("password for connection to mysql server")
    @Config("mysql-event-listener.connection-password")
    public MysqlEventListenerConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }
}
