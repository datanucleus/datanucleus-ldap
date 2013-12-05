/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
2008 Stefan Seelmann - connection pooling "JNDI"
    ...
 **********************************************************************/
package org.datanucleus.store.ldap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.transaction.xa.XAResource;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;

/**
 * Implementation of a ConnectionFactory for LDAP.
 * Transactional : Holds a "connection" for the duration of the transaction.
 * Non-transactional : Obtains the "connection" and closes it after the operation.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    Hashtable ldapEnv = null;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // Build up LDAP properties for obtaining connections
        ldapEnv = new Hashtable();
        ldapEnv.put(Context.INITIAL_CONTEXT_FACTORY, storeMgr.getConnectionDriverName());
        ldapEnv.put(Context.PROVIDER_URL, storeMgr.getConnectionURL());
        ldapEnv.put(Context.SECURITY_PRINCIPAL, storeMgr.getConnectionUserName());
        ldapEnv.put(Context.SECURITY_CREDENTIALS, storeMgr.getConnectionPassword());
        if ("JNDI".equals(storeMgr.getStringProperty("datanucleus.connectionPoolingType")))
        {
            ldapEnv.put("com.sun.jndi.ldap.connect.pool", "true");
            // System.setProperty("com.sun.jndi.ldap.connect.pool.debug", "fine");

            if (storeMgr.hasProperty("datanucleus.connectionPool.maxPoolSize"))
            {
                int size = storeMgr.getIntProperty("datanucleus.connectionPool.maxPoolSize");
                if (size >= 0)
                {
                    System.setProperty("com.sun.jndi.ldap.connect.pool.maxsize", "" + size);
                }
            }
            if (storeMgr.hasProperty("datanucleus.connectionPool.initialPoolSize"))
            {
                int size = storeMgr.getIntProperty("datanucleus.connectionPool.initialPoolSize");
                if (size >= 0)
                {
                    System.setProperty("com.sun.jndi.ldap.connect.pool.initsize", "" + size);
                }
            }
        }

        MetaDataManager mdm = storeMgr.getNucleusContext().getMetaDataManager();
        Collection<String> classesWithMetaData = mdm.getClassesWithMetaData();
        StringBuilder sb = new StringBuilder();
        for (String className : classesWithMetaData)
        {
            AbstractClassMetaData cmd = mdm.getMetaDataForClass(className, storeMgr.getNucleusContext().getClassLoaderResolver(null));
            List<AbstractMemberMetaData> mmds = LDAPUtils.getAllMemberMetaData(cmd);
            for (AbstractMemberMetaData mmd : mmds)
            {
                if (mmd.getJoinMetaData() == null && mmd.getType().isArray())
                {
                    String attributeNameForField = LDAPUtils.getAttributeNameForField(mmd);
                    sb.append(attributeNameForField).append(' ');
                }
            }
        }
        if (sb.length() > 0)
        {
            ldapEnv.put("java.naming.ldap.attributes.binary", sb.toString());
        }
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction
     * associated to the ExecutionContext
     * @param ec ExecutionContext that this connection is for (or null)
     * @param txnOptionsIgnored Any options for creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map txnOptionsIgnored)
    {
        return new ManagedConnectionImpl();
    }

    /**
     * Implementation of a ManagedConnection for LDAP.
     */
    public class ManagedConnectionImpl extends AbstractManagedConnection
    {
        LdapContext mainContext;

        List<LdapContext> subContexts;

        public ManagedConnectionImpl()
        {
        }

        public Object getConnection()
        {
            if (conn == null)
            {
                try
                {
                    mainContext = new InitialLdapContext(ldapEnv, null);
                    subContexts = new ArrayList<LdapContext>();
                    conn = mainContext;
                }
                catch (NamingException e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }
            }

            try
            {
                // create a new instance of the main context to ensure thread safety
                LdapContext subContext = mainContext.newInstance(null);
                subContexts.add(subContext);
                return subContext;
            }
            catch (NamingException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }

        public void release()
        {
            if (commitOnRelease)
            {
                // Non-transactional operation end : write to LDAP and close context(s)
                try
                {
                    for (LdapContext subContext : subContexts)
                    {
                        subContext.close();
                    }
                    mainContext.close();
                    subContexts = null;
                    mainContext = null;
                    conn = null;
                }
                catch (Exception e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }
            }
            super.release();
        }

        public void close()
        {
            if (mainContext == null)
            {
                return;
            }

            for (int i=0; i<listeners.size(); i++)
            {
                listeners.get(i).managedConnectionPreClose();
            }

            try
            {
                try
                {
                    for (LdapContext subContext : subContexts)
                    {
                        subContext.close();
                    }
                    mainContext.close();
                }
                catch (Exception e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }

            }
            finally
            {
                subContexts = null;
                mainContext = null;
                conn = null;

                for (int i=0; i<listeners.size(); i++)
                {
                    listeners.get(i).managedConnectionPostClose();
                }
            }
        }

        public XAResource getXAResource()
        {
            return null;
        }
    }
}