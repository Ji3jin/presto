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

import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Path("/v1/catalog")
public class CatalogResource
{
    private final DynamicCatalogStoreConfig config;

    @Inject
    public CatalogResource(DynamicCatalogStoreConfig config)
    {
        this.config = requireNonNull(config, "dynamicCatalogStoreConfig is null");
    }

    @GET
    @Path("test")
    public Response test()
    {
        return Response.ok("Hello Presto").build();
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createCatalog(CatalogInfo catalogInfo)
    {
        requireNonNull(catalogInfo, "catalogInfo is null");
        JsonCodec<Map> codec = JsonCodec.jsonCodec(Map.class);
        try (Connection connection = ConnectionConfig.openConnection(config);
                PreparedStatement preparedStatement = connection.prepareStatement(ConnectionConfig.insertCatalogSql)) {
            preparedStatement.setString(1, catalogInfo.getCatalogName());
            preparedStatement.setString(2, catalogInfo.getConnectorName());
            preparedStatement.setString(3, catalogInfo.getCreator());
            preparedStatement.setString(4, codec.toJson(catalogInfo.getProperties()));
            preparedStatement.executeUpdate();
        }
        catch (SQLException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.status(Response.Status.OK).build();
    }
}
