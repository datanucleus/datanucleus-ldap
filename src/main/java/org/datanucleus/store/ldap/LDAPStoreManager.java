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
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.flush.FlushOrdered;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.ldap.query.JDOQLQuery;
import org.datanucleus.store.ldap.query.JPQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.Localiser;

/**
 * Manager for LDAP datastores.
 */
public class LDAPStoreManager extends AbstractStoreManager
{
    /** Extension for metadata of a field/property to define the mapping strategy to use. */
    public static final String MAPPING_STRATEGY_EXTENSON = "mapping-strategy";

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
    public synchronized void close()
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
        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_DELETE);
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_DELETE);
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec)
    {
        if (language.equalsIgnoreCase("JDOQL"))
        {
            return new JDOQLQuery(this, ec);
        }
        else if (language.equalsIgnoreCase("JPQL"))
        {
            return new JPQLQuery(this, ec);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, java.lang.String)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, String queryString)
    {
        if (language.equalsIgnoreCase("JDOQL"))
        {
            return new JDOQLQuery(this, ec, queryString);
        }
        else if (language.equalsIgnoreCase("JPQL"))
        {
            return new JPQLQuery(this, ec, queryString);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, org.datanucleus.store.query.Query)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, Query q)
    {
        if (language.equalsIgnoreCase("JDOQL"))
        {
            return new JDOQLQuery(this, ec, (JDOQLQuery) q);
        }
        else if (language.equalsIgnoreCase("JPQL"))
        {
            return new JPQLQuery(this, ec, (JPQLQuery) q);
        }
        throw new NucleusException("Error creating query for language " + language);
    }
}