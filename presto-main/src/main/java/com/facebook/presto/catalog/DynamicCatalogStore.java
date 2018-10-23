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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.ConnectorManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.Announcer;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.catalog.AnnouncementUtils.removeConnectorIdAnnouncement;
import static com.facebook.presto.catalog.AnnouncementUtils.updateConnectorIdAnnouncement;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    //catalog info cache(key:md5sum(toJson(catalogInfo)),value: catalogInfo)
    private final Map<String, CatalogInfo> catalogInfoCache = new HashMap<>();
    private final DynamicCatalogStoreConfig config;
    private final ScheduledExecutorService scheduledService;
    private final Announcer announcer;
    private final CatalogLoader catalogLoader;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, DynamicCatalogStoreConfig config,
                               Announcer announcer)
    {
        this.connectorManager = connectorManager;
        this.config = config;
        this.disabledCatalogs = ImmutableSet
                .copyOf(firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()));
        this.scheduledService = Executors.newSingleThreadScheduledExecutor();
        this.announcer = announcer;
        catalogLoader = new CatalogLoader(config);
    }

    public boolean areCatalogsLoaded()
    {
        return catalogsLoaded.get();
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        load();

        scheduledService.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                reload();
            }
        }, 60, this.config.getCatalogDetectTimeInterval(), TimeUnit.SECONDS);

        log.info("Catalog loader scheduled thread start with time interval %d s",
                this.config.getCatalogDetectTimeInterval());
        catalogsLoaded.set(true);
    }

    public void load() throws Exception
    {
        Map<String, CatalogInfo> catalogInfoMap = catalogLoader.load();
        catalogInfoMap.forEach((key, catalogInfo) -> {
            loadCatalog(catalogInfo);
        });
        catalogInfoCache.putAll(catalogInfoMap);
    }

    public void reload()
    {
        try {
            Map<String, CatalogInfo> catalogInfoMap = catalogLoader.load();
            Map<String, CatalogInfo> deletedCatalogMap = getDeletedCatalog(catalogInfoCache,
                    catalogInfoMap);
            Map<String, CatalogInfo> addedCatalogMap = getAddedCatalog(catalogInfoCache, catalogInfoMap);
            deletedCatalogMap.forEach((key, catalogInfo) -> {
                connectorManager.dropConnection(catalogInfo.getCatalogName());
                removeConnectorIdAnnouncement(announcer, new ConnectorId(catalogInfo.getCatalogName()));
                catalogInfoCache.remove(key);
                log.info("removed catalog %s with connector %s", catalogInfo.getCatalogName(),
                        catalogInfo.getConnectorName());
            });
            addedCatalogMap.forEach((key, catalogInfo) -> {
                ConnectorId connectorId = loadCatalog(catalogInfo);
                if (connectorId != null) {
                    updateConnectorIdAnnouncement(announcer, connectorId);
                    catalogInfoCache.put(key, catalogInfo);
                }
            });
        }
        catch (Exception e) {
            log.error(e);
        }
    }

    private Map<String, CatalogInfo> getDeletedCatalog(Map<String, CatalogInfo> originCatalogMap,
                                                       Map<String, CatalogInfo> newCatalogMap)
    {
        ImmutableMap.Builder builder = ImmutableMap.builder();
        originCatalogMap.forEach((key, catalogInfo) -> {
            if (!newCatalogMap.containsKey(key)) {
                builder.put(key, catalogInfo);
            }
        });
        return builder.build();
    }

    private Map<String, CatalogInfo> getAddedCatalog(Map<String, CatalogInfo> originCatalogMap,
                                                     Map<String, CatalogInfo> newCatalogMap)
    {
        ImmutableMap.Builder builder = ImmutableMap.builder();
        newCatalogMap.forEach((key, catalogInfo) -> {
            if (!originCatalogMap.containsKey(key)) {
                builder.put(key, catalogInfo);
            }
        });
        return builder.build();
    }

    private ConnectorId loadCatalog(CatalogInfo catalogInfo)
    {
        String catalogName = catalogInfo.getCatalogName();
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return null;
        }
        log.info("Loading catalog %s", catalogInfo.getCatalogName());

        String connectorName = catalogInfo.getConnectorName();
        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name",
                catalogInfo.getConnectorName());
        ConnectorId connectorId = connectorManager.createConnection(catalogName, connectorName,
                ImmutableMap.copyOf(catalogInfo.getProperties()));
        log.info("Added catalog %s using connector %s", catalogName, connectorName);
        return connectorId;
    }

    @PreDestroy
    public void destroy()
    {
        scheduledService.shutdown();
    }
}
