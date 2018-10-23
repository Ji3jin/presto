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
package com.facebook.presto.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CatalogInfo
{
    private final String catalogName;

    private final String connectorName;
    private final String creator;

    private final Map<String, String> properties;

    @JsonCreator
    public CatalogInfo(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("connectorName") String connectorName,
            @JsonProperty("creator") String creator,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.creator = requireNonNull(creator, "creator is null");
        this.properties = requireNonNull(properties, "properties is null");
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public String getConnectorName()
    {
        return connectorName;
    }

    @JsonProperty
    public String getCreator()
    {
        return creator;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }
}
