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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.json.JsonCodec.jsonCodec;

public class CatalogFileLoader
        extends CatalogLoader
{
    public CatalogFileLoader(DynamicCatalogStoreConfig config)
    {
        super(config);
    }

    @Override
    public Map<String, CatalogInfo> load()
            throws Exception
    {
        ImmutableMap.Builder builder = ImmutableMap.builder();
        JsonCodec<CatalogInfo> catalogJsonCodec = jsonCodec(CatalogInfo.class);
        for (File file : listFiles(this.config.getCatalogConfigurationDir())) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                String catalogName = Files.getNameWithoutExtension(file.getName());
                Map<String, String> properties = new HashMap<>(loadProperties(file));
                String connectorName = properties.remove("connector.name");
                checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());
                if (!Strings.isNullOrEmpty(catalogName)
                        && !Strings.isNullOrEmpty(connectorName)) {
                    CatalogInfo catalogInfo = new CatalogInfo(catalogName, connectorName, ConnectionConfig.defaultCreator, properties);
                    String md5 = Hashing.md5().hashBytes(catalogJsonCodec.toJsonBytes(catalogInfo)).toString();
                    builder.put(md5, catalogInfo);
                }
            }
        }
        return builder.build();
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}