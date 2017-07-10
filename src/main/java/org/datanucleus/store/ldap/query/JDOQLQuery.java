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
2008 Andy Jefferson - Extract LDAP specific code out into LDAPUtils
    ...
 ***********************************************************************/
package org.datanucleus.store.ldap.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.query.inmemory.JDOQLInMemoryEvaluator;
import org.datanucleus.query.inmemory.JavaQueryInMemoryEvaluator;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * JDOQL query for LDAP datastores. There are two modes:
 * <ul>
 * <li>If the extension datanucleus.query.evaluateInMemory is set to true the query filter is evaluated in-memory. This
 * means only a simple LDAP search is performed for all objects of the requisite "distinguished name" to limit it to the
 * candidate class. Then utilises the generic JDOQLEvaluator to do in-memory imposition of the JDOQL filter, ordering,
 * result etc.</li>
 * <li>If the extension is absent a more specific LDAP search is performed in order to limit the number of matching
 * objects on the LDAP server side. Nevertheless the resulting candidates are additionally evaluated the in-memory. One
 * reason is that most LDAP attributes are case insensitive so an LDAP search may return more objects and they must be
 * filtered additionally using the in-memory evaluator.</li>
 * </ul>
 * Performs a simple LDAP search for all objects of the requisite "distinguished name" to limit it to the candidate
 * class. Then utilises the generic JDOQLEvaluator to do in-memory imposition of the JDOQL filter, ordering, result etc.
 */
public class JDOQLQuery extends AbstractJDOQLQuery
{
    private static final long serialVersionUID = -7781024972450929587L;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec the associated ExecutionContext for this query.
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JDOQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec The ExecutionContext
     * @param q The query from which to copy criteria.
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, JDOQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec The persistence manager
     * @param query The query string
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    protected Object performExecute(Map parameters)
    {
        boolean inMemory = evaluateInMemory();
        long startTime = 0;
        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            startTime = System.currentTimeMillis();
            NucleusLogger.QUERY.debug(Localiser.msg("021046", Query.LANGUAGE_JDOQL, getSingleStringQuery(), null));
        }
        Collection candidates = null;
        if (candidateCollection == null)
        {
            candidates = LDAPUtils.getObjectsOfCandidateType(getStoreManager(), ec, compilation, parameters, 
                candidateClass, subclasses, ignoreCache, inMemory);
        }
        else
        {
            candidates = new ArrayList(candidateCollection);
        }

        // Map any result restrictions onto the LDAP search results
        JavaQueryInMemoryEvaluator resultMapper = new JDOQLInMemoryEvaluator(this, candidates, compilation, parameters, 
            ec.getClassLoaderResolver());
        Collection results = resultMapper.execute(true, true, true, true, true);

        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            NucleusLogger.QUERY.debug(Localiser.msg("021074", Query.LANGUAGE_JDOQL, "" + (System.currentTimeMillis() - startTime)));
        }

        return results;
    }
}