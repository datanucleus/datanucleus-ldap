/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
2008 Andy Jefferson - abstracted methods up to AbstractStoreManager
 ...
***********************************************************************/
package org.datanucleus.store.ldap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.flush.FlushOrdered;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.util.Localiser;

/**
 * Manager for LDAP datastores.
 */
public class LDAPStoreManager extends AbstractStoreManager
{
    static
    {
        Localiser.registerBundle("org.datanucleus.store.ldap.Localisation", LDAPStoreManager.class.getClassLoader());
    }

    MetaDataListener metadataListener;

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param ctx context
     * @param props Properties for the datastore
     */
    public LDAPStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
    {
        super("ldap", clr, ctx, props);

        // Handler for metadata
        metadataListener = new LDAPMetaDataListener();
        ctx.getMetaDataManager().registerListener(metadataListener);

        // Handler for persistence process
        persistenceHandler = new LDAPPersistenceHandler(this);
        flushProcess = new FlushOrdered();

        logConfiguration();
    }

    /**
     * Release of resources
     */
    public void close()
    {
        nucleusContext.getMetaDataManager().deregisterListener(metadataListener);
        super.close();
    }

    /**
     * Accessor for the supported options in string form.
     * @return Supported options
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC);
        return set;
    }
}