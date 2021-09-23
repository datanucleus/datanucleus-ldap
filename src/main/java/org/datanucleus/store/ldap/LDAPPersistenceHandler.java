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
2008 Stefan Seelmann - locateObject retrieve no attributes
    ...
 **********************************************************************/
package org.datanucleus.store.ldap;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.naming.ContextNotEmptyException;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ElementMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.ldap.LDAPUtils.LocationInfo;
import org.datanucleus.store.ldap.fieldmanager.AbstractMappingStrategy;
import org.datanucleus.store.ldap.fieldmanager.FetchFieldManager;
import org.datanucleus.store.ldap.fieldmanager.MappingStrategyHelper;
import org.datanucleus.store.ldap.fieldmanager.StoreFieldManager;
import org.datanucleus.store.ldap.fieldmanager.RelationByAttributeMetaData;
import org.datanucleus.store.ldap.fieldmanager.RelationByDnMetaData;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Persistence handler for persisting to LDAP datastores. 
 * Doesn't support optimistic version checking. Doesn't support datastore identity.
 */
public class LDAPPersistenceHandler extends AbstractPersistenceHandler
{
    /**
     * Constructor.
     * @param storeMgr Manager for the datastore
     */
    public LDAPPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /**
     * Method to close the handler and release any resources.
     */
    public void close()
    {
    }

    /**
     * Insert the object managed by the passed ObjectProvider into the LDAP datastore.
     * @param sm StateManager
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void insertObject(final ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        // pre-insert
        // for hierarchical mapping: ensure that parent is created before child entry
        AbstractClassMetaData cmd = sm.getClassMetaData();
        if (LDAPUtils.isHierarchicalMappedAtChild(sm))
        {
            LocationInfo locationInfo = LDAPUtils.getLocationInfo(cmd);
            AbstractMemberMetaData parentFieldMmd = cmd.getMetaDataForMember(locationInfo.parentFieldName);
            Object parentFieldValue = sm.provideField(parentFieldMmd.getAbsoluteFieldNumber());
            if (parentFieldValue != null)
            {
                // compose DN using parent DN
                sm.getExecutionContext().findObjectProvider(parentFieldValue, true);
            }
            else if (locationInfo.dn == null)
            {
                throw new NucleusUserException(Localiser.msg("LDAP.Insert.MissingParentReference", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }
        }

        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // LDAP enforces application identity, throws a NameAlreadyBoundException
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            throw new NucleusUserException(Localiser.msg("LDAP.DatastoreID"));
        }

        Set<String> objectClasses = LDAPUtils.getObjectClassesForClass(sm.getClassMetaData());
        if (objectClasses == null)
        {
            throw new NucleusDataStoreException("Missing 'objectClass' extension or 'schema' attribute for class " + cmd.getName());
        }

        // insert
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.Insert.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            DirContext ctx = (DirContext) mconn.getConnection();
            BasicAttributes attrs = new BasicAttributes();

            // split embedded members
            List<AbstractMemberMetaData> nonEmbeddedMmds = LDAPUtils.getAllMemberMetaData(cmd);
            List<AbstractMemberMetaData> embeddedMmds = new ArrayList<AbstractMemberMetaData>();
            for (Iterator<AbstractMemberMetaData> it = nonEmbeddedMmds.iterator(); it.hasNext();)
            {
                AbstractMemberMetaData mmd = it.next();
                if (LDAPUtils.isEmbeddedField(mmd))
                {
                    embeddedMmds.add(mmd);
                    it.remove();
                }
            }
            int[] nonEmbeddedFieldNumbers = new int[nonEmbeddedMmds.size()];
            for (int i = 0; i < nonEmbeddedFieldNumbers.length; i++)
            {
                nonEmbeddedFieldNumbers[i] = nonEmbeddedMmds.get(i).getAbsoluteFieldNumber();
            }
            int[] embeddedFieldNumbers = new int[embeddedMmds.size()];
            for (int i = 0; i < embeddedFieldNumbers.length; i++)
            {
                embeddedFieldNumbers[i] = embeddedMmds.get(i).getAbsoluteFieldNumber();
            }

            // 1st: non-embedded members
            sm.provideFields(nonEmbeddedFieldNumbers, new StoreFieldManager(storeMgr, sm, attrs, true));
            LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, false);
            BasicAttribute objectClass = new BasicAttribute("objectClass");
            for (String oc : objectClasses)
            {
                objectClass.add(oc);
            }
            attrs.put(objectClass);
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.createSubcontext", dn, attrs));
            }
            // use bind() method here
            // createSubContext returns a new Context object leads to problems when using connection pooling
            ctx.bind(dn, null, attrs);

            // 2nd: embedded members now
            // insert embedded child objects
            // insertEmbeddedChildEntries(sm, ctx);
            attrs = new BasicAttributes();
            sm.provideFields(embeddedFieldNumbers, new StoreFieldManager(storeMgr, sm, attrs, true));
            if (attrs.size() > 0)
            {
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.modifyAttributes", dn, "REPLACE", attrs));
                }
                ctx.modifyAttributes(dn, DirContext.REPLACE_ATTRIBUTE, attrs);
            }

            // TODO Implement version retrieval
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE.debug(Localiser.msg("LDAP.Insert.ObjectPersisted", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }
        }
        catch (NameAlreadyBoundException e)
        {
            throw new NucleusUserException(Localiser.msg("LDAP.Insert.ObjectWithIdAlreadyExists",
                sm.getObjectAsPrintable(), sm.getInternalObjectId()), e);
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Updates the specified fields of the object managed by the passed ObjectProvider in the LDAP datastore.
     * @param sm StateManager
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails
     */
    public void updateObject(final ObjectProvider sm, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        // TODO Implement version checking
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                AbstractClassMetaData cmd = sm.getClassMetaData();
                StringBuilder fieldStr = new StringBuilder();
                for (int i = 0; i < fieldNumbers.length; i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.Update.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId(), fieldStr.toString()));
            }

            DirContext ctx = (DirContext) mconn.getConnection();

            // pre-update
            // for hierarchical mapping: check if parent has been changed
            // in that case move the entry to the other tree (moddn operation)
            AbstractClassMetaData cmd = sm.getClassMetaData();
            if (LDAPUtils.isHierarchicalMappedAtChild(sm))
            {
                LocationInfo locationInfo = LDAPUtils.getLocationInfo(cmd);
                AbstractMemberMetaData parentFieldMmd = cmd.getMetaDataForMember(locationInfo.parentFieldName);
                int absoluteFieldNumber = parentFieldMmd.getAbsoluteFieldNumber();
                for (int i : fieldNumbers)
                {
                    if (i == absoluteFieldNumber)
                    {
                        // TODO: manual move for non-leaf entry?
                        LdapName oldDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, true);
                        sm.setAssociatedValue("dn", null);
                        LdapName newDn = null;

                        Object parentFieldValue = sm.provideField(absoluteFieldNumber);
                        if (parentFieldValue != null)
                        {
                            newDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, false);
                        }
                        else if (locationInfo.dn != null)
                        {
                            // construct new DN without parent -> put to fixed DN
                            newDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, false);
                        }

                        if (newDn != null && !oldDn.equals(newDn))
                        {
                            LDAPUtils.markForRename(storeMgr, sm.getObject(), ec, oldDn, newDn);
                            LDAPUtils.unmarkForDeletion(sm.getObject(), ec);
                        }
                        break;
                    }
                }
            }

            // update
            final BasicAttributes attrs = new BasicAttributes();
            sm.provideFields(fieldNumbers, new StoreFieldManager(storeMgr, sm, attrs, false));
            LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, true);
            // replace attributes, empty attribute deletes existing attribute
            if (attrs.size() > 0)
            {
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.modifyAttributes", dn, "REPLACE", attrs));
                }
                ctx.modifyAttributes(dn, DirContext.REPLACE_ATTRIBUTE, attrs);
            }
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Deletes the object managed by the passed ObjectProvider from the LDAP datastore.
     * @param sm StateManager
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails on an optimistic transaction for this object
     */
    public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);
        NucleusLogger.GENERAL.info(">> deleteObject for " + sm);

        // Delete all reachable PC objects (due to dependent-field)
        sm.loadUnloadedFields();
        sm.provideFields(sm.getClassMetaData().getAllMemberPositions(), new DeleteFieldManager(sm));

        // referential integrity: check if this sm is referenced from somewhere in that case remove the reference
        deleteDnReferences(sm);
        deleteAttributeReferences(sm);

        // delete
        // TODO Implement version checking
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        DirContext ctx = (DirContext) mconn.getConnection();
        long startTime = System.currentTimeMillis();
        try
        {
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.Delete.Start", sm.getObjectAsPrintable(), sm
                        .getInternalObjectId()));
            }

            LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, true);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.destroySubcontext", dn));
            }
            ctx.unbind(dn);

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }
        }
        catch (ContextNotEmptyException cnee)
        {
            // delete recursive if parent of hierarchical or cascade-delete
            try
            {
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.DeleteRecursive.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                }

                LDAPUtils.deleteRecursive(LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, true), ctx);

                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.ExecutionTime", (System.currentTimeMillis() - startTime)));
                }
            }
            catch (NamingException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Deletes DN references to the given state manager
     * @param sm StateManager
     */
    private void deleteDnReferences(ObjectProvider sm)
    {
        // 1st) this sm itself has a DN reference with <element>
        int[] fieldNumbers = sm.getClassMetaData().getAllMemberPositions();
        for (int fieldNumber : fieldNumbers)
        {
            AbstractMemberMetaData mmd = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            if (RelationByDnMetaData.isRelationByDn(mmd, sm.getExecutionContext().getMetaDataManager()))
            {
                ElementMetaData elementMetaData = mmd.getElementMetaData();
                if (elementMetaData != null)
                {
                    AbstractClassMetaData otherCmd = LDAPUtils.getEffectiveClassMetaData(mmd, sm.getExecutionContext().getMetaDataManager());

                    Class c = sm.getExecutionContext().getClassLoaderResolver().classForName(otherCmd.getFullClassName());
                    if (c.isInterface() || Modifier.isAbstract(c.getModifiers()))
                    {
                        continue;
                    }

                    String name = elementMetaData.getColumnName() != null ? elementMetaData.getColumnName() : elementMetaData
                            .getColumnMetaData()[0].getName();
                    String emptyValue = LDAPUtils.getEmptyValue(mmd);
                    deleteDnReference(otherCmd, name, sm, emptyValue);
                }
            }
        }

        // 2nd) any other object has a DN reference to this sm w/o <element>
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        MetaDataManager mdm = sm.getExecutionContext().getMetaDataManager();
        Collection<String> classesWithMetaData = mdm.getClassesWithMetaData();
        for (String className : classesWithMetaData)
        {
            AbstractClassMetaData otherCmd = mdm.getMetaDataForClass(className, clr);
            if (otherCmd.isEmbeddedOnly())
            {
                continue;
            }
            Class c = sm.getExecutionContext().getClassLoaderResolver().classForName(className);
            if (c.isInterface() || Modifier.isAbstract(c.getModifiers()))
            {
                continue;
            }
            fieldNumbers = otherCmd.getAllMemberPositions();
            for (int fieldNumber : fieldNumbers)
            {
                AbstractMemberMetaData mmd = otherCmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
                if (RelationByDnMetaData.isRelationByDn(mmd, sm.getExecutionContext().getMetaDataManager()))
                {
                    AbstractClassMetaData effectiveCmd = LDAPUtils.getEffectiveClassMetaData(mmd, sm.getExecutionContext().getMetaDataManager());
                    String[] subclassNames = effectiveCmd != null ? sm.getExecutionContext().getMetaDataManager().getSubclassesForClass(effectiveCmd.getFullClassName(), true) : null;
                    if (effectiveCmd == sm.getClassMetaData() || (subclassNames != null && Arrays.asList(subclassNames).contains(
                        sm.getClassMetaData().getFullClassName())))
                    {
                        String name = LDAPUtils.getAttributeNameForField(mmd);
                        String emptyValue = LDAPUtils.getEmptyValue(mmd);
                        deleteDnReference(otherCmd, name, sm, emptyValue);
                    }
                }
            }
        }
    }

    /**
     * Deletes the DN reference from all classes of given class meta data to the given state manager
     * @param cmd the class meta data of the object where the reference should be removed from
     * @param sm StateManager
     * @param emptyValue the value used for an empty member attribute
     */
    private void deleteDnReference(AbstractClassMetaData cmd, String name, ObjectProvider sm, Object emptyValue)
    {
        // System.out.println("deleteDnReference: " + cmd.getName() + " - " + sm);
        LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, true);

        // search for object with (pcAttributeName=myDN)
        ExecutionContext om = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(om);
        try
        {
            LdapName base = LDAPUtils.getSearchBase(cmd, sm.getExecutionContext().getMetaDataManager());
            String ocFilter = LDAPUtils.getSearchFilter(cmd);
            String dnFilter = "(" + name + "=" + dn.toString() + ")";
            String filter = ocFilter != null ? "(&" + ocFilter + dnFilter + ")" : dnFilter;
            SearchControls searchControls = LDAPUtils.getSearchControls(cmd);
            searchControls.setReturningAttributes(new String[]{name});
            DirContext ctx = (DirContext) mconn.getConnection();
            NamingEnumeration<SearchResult> enumeration = ctx.search(base, filter, searchControls);
            while (enumeration.hasMoreElements())
            {
                SearchResult sr = enumeration.nextElement();
                String srName = sr.getNameInNamespace();
                Attributes attrs = sr.getAttributes();
                Attribute attr = sr.getAttributes().get(name);
                if (attr != null)
                {
                    // TODO: should not compare strings but parsed LdapName objects
                    boolean removed = attr.remove(dn.toString());
                    if (removed)
                    {
                        if (attr.size() == 0 && emptyValue != null)
                        {
                            attr.add(emptyValue);
                        }
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.DeleteDnReference", srName, dn));
                        }
                        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.modifyAttributes", srName, "REPLACE", attrs));
                        }
                        ctx.modifyAttributes(srName, DirContext.REPLACE_ATTRIBUTE, attrs);
                    }
                }
            }
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Deletes attribute references to the given ObjectProvider
     * @param sm StateManager
     */
    private void deleteAttributeReferences(ObjectProvider sm)
    {
        // 1st) this sm itself has a attribute reference with <element>
        int[] fieldNumbers = sm.getClassMetaData().getAllMemberPositions();
        for (int fieldNumber : fieldNumbers)
        {
            AbstractMemberMetaData mmd = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            if (RelationByAttributeMetaData.isRelationByAttribute(mmd, sm.getExecutionContext().getMetaDataManager()))
            {
                ElementMetaData elementMetaData = mmd.getElementMetaData();
                if (elementMetaData != null)
                {
                    RelationByAttributeMetaData mappingMetaData = new RelationByAttributeMetaData(mmd, sm.getExecutionContext().getMetaDataManager());
                    if (mappingMetaData.getNonOwnerMMD() == mmd)
                    {
                        String ownerAttributeName = mappingMetaData.getOwnerAttributeName();
                        String joinAttributeName = mappingMetaData.getJoinAttributeName();
                        Object joinAttributeValue = LDAPUtils.getAttributeValue(storeMgr, sm, joinAttributeName);
                        String emptyValue = LDAPUtils.getEmptyValue(mmd);
                        AbstractClassMetaData otherCmd = LDAPUtils.getEffectiveClassMetaData(mmd, sm.getExecutionContext().getMetaDataManager());
                        deleteAttributeReference(otherCmd, ownerAttributeName, joinAttributeValue, sm, emptyValue);
                    }
                }
            }
        }

        // 2nd) any other object has an attribute reference to this sm w/o <element>
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        MetaDataManager mdm = sm.getExecutionContext().getMetaDataManager();
        Collection<String> classesWithMetaData = mdm.getClassesWithMetaData();
        for (String className : classesWithMetaData)
        {
            AbstractClassMetaData otherCmd = mdm.getMetaDataForClass(className, clr);
            if (otherCmd.isEmbeddedOnly())
            {
                continue;
            }
            Class c = sm.getExecutionContext().getClassLoaderResolver().classForName(className);
            if (c.isInterface() || Modifier.isAbstract(c.getModifiers()))
            {
                continue;
            }
            fieldNumbers = otherCmd.getAllMemberPositions();
            for (int fieldNumber : fieldNumbers)
            {
                AbstractMemberMetaData mmd = otherCmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
                if (RelationByAttributeMetaData.isRelationByAttribute(mmd, sm.getExecutionContext().getMetaDataManager()))
                {
                    AbstractClassMetaData effectiveCmd = LDAPUtils.getEffectiveClassMetaData(mmd, sm.getExecutionContext().getMetaDataManager());
                    String[] subclassNames = effectiveCmd != null ? sm.getExecutionContext().getMetaDataManager().getSubclassesForClass(
                        effectiveCmd.getFullClassName(), true) : null;
                    if (effectiveCmd == sm.getClassMetaData() || (subclassNames != null && Arrays.asList(subclassNames).contains(
                        sm.getClassMetaData().getFullClassName())))
                    {
                        RelationByAttributeMetaData mappingMetaData = new RelationByAttributeMetaData(mmd, sm.getExecutionContext().getMetaDataManager());
                        if (mappingMetaData.getOwnerMMD() == mmd)
                        {
                            String ownerAttributeName = mappingMetaData.getOwnerAttributeName();
                            String joinAttributeName = mappingMetaData.getJoinAttributeName();
                            Object joinAttributeValue = LDAPUtils.getAttributeValue(storeMgr, sm, joinAttributeName);
                            String emptyValue = LDAPUtils.getEmptyValue(mmd);
                            deleteAttributeReference(otherCmd, ownerAttributeName, joinAttributeValue, sm, emptyValue);
                        }
                    }
                }
            }
        }
    }

    /**
     * Deletes the DN reference from all classes of given class meta data to the given state manager
     * @param cmd the class meta data of the object where the reference should be removed from
     * @param attributeName the attribute name to search for
     * @param attributeValue the attribute value to search for
     * @param sm StateManager
     * @param emptyValue the value used for an empty member attribute
     */
    private void deleteAttributeReference(AbstractClassMetaData cmd, String attributeName, Object attributeValue, ObjectProvider sm, Object emptyValue)
    {
        // search for object with (pcAttributeName=myDN)
        ExecutionContext om = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(om);
        try
        {
            LdapName base = LDAPUtils.getSearchBase(cmd, sm.getExecutionContext().getMetaDataManager());
            String ocFilter = LDAPUtils.getSearchFilter(cmd);
            String dnFilter = "(" + attributeName + "=" + attributeValue + ")";
            String filter = ocFilter != null ? "(&" + ocFilter + dnFilter + ")" : dnFilter;
            SearchControls searchControls = LDAPUtils.getSearchControls(cmd);
            searchControls.setReturningAttributes(new String[]{attributeName});
            DirContext ctx = (DirContext) mconn.getConnection();
            NamingEnumeration<SearchResult> enumeration = ctx.search(base, filter, searchControls);
            while (enumeration.hasMoreElements())
            {
                SearchResult sr = enumeration.nextElement();
                String srName = sr.getNameInNamespace();
                Attributes attrs = sr.getAttributes();
                Attribute attr = sr.getAttributes().get(attributeName);
                if (attr != null)
                {
                    boolean removed = attr.remove(attributeValue);
                    if (removed)
                    {
                        if (attr.size() == 0 && emptyValue != null)
                        {
                            attr.add(emptyValue);
                        }
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.DeleteAttributeReference", attributeName,
                                attributeValue, srName));
                        }
                        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.modifyAttributes", srName, "REPLACE", attrs));
                        }
                        ctx.modifyAttributes(srName, DirContext.REPLACE_ATTRIBUTE, attrs);
                    }
                }
            }
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to retrieve the specified fields of the object managed by StateManager.
     * @param sm StateManager
     * @param fieldNumbers Absolute field numbers to retrieve
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void fetchObject(final ObjectProvider sm, int[] fieldNumbers)
    {
        if (sm.getLifecycleState().isDeleted())
        {
            return;
        }

        AbstractClassMetaData cmd = sm.getClassMetaData();
        if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            StringBuilder str = new StringBuilder("Fetching object \"");
            str.append(sm.getObjectAsPrintable()).append("\" (id=");
            str.append(sm.getInternalObjectId()).append(")").append(" fields [");
            for (int i = 0; i < fieldNumbers.length; i++)
            {
                if (i > 0)
                {
                    str.append(",");
                }
                str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
            }
            str.append("]");
            NucleusLogger.DATASTORE_RETRIEVE.debug(str.toString());
        }

        List<String> attributeNameList = new ArrayList<String>();
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        for (int i = 0; i < fieldNumbers.length; i++)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
            RelationType relType = mmd.getRelationType(clr);
            if (relType == RelationType.NONE)
            {
                String attrName = LDAPUtils.getAttributeNameForField(mmd);
                attributeNameList.add(attrName);
            }
            else
            {
                AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, sm, mmd, new BasicAttributes());
                if (ms != null)
                {
                    List<String> attributeNames = ms.getAttributeNames();
                    attributeNameList.addAll(attributeNames);
                }
            }
        }
        String[] attributeNames = attributeNameList.toArray(new String[0]);

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            DirContext ctx = (DirContext) mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("LDAP.Fetch.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, true);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.getAttributes", dn, attributeNameList, ""));
            }

            final Attributes result = ctx.getAttributes(dn, attributeNames);
            sm.replaceFields(fieldNumbers, new FetchFieldManager(storeMgr, sm, result));

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("LDAP.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumReads();
                ec.getStatistics().incrementFetchCount();
            }
        }
        catch (NameNotFoundException e)
        {
            throw new NucleusObjectNotFoundException("Object not found", sm.getInternalObjectId());
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Accessor for the object with the specified identity (if present). Since we don't manage the memory instantiation
     * of objects this just returns null.
     * @param ec ExecutionContext in use
     * @param id Identity of the object
     * @return The object
     */
    public Object findObject(ExecutionContext ec, Object id)
    {
        return null;
    }

    /**
     * Locates the object managed by the passed ObjectProvider into the LDAP datastore.
     * @param sm StateManager
     * @throws NucleusObjectNotFoundException if the object cannot be located
     */
    public void locateObject(ObjectProvider sm)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                long startTime = System.currentTimeMillis();
                if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("LDAP.Locate.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                }
                DirContext ctx = (DirContext) mconn.getConnection();
                LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, true);
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.getAttributes", dn, "none", ""));
                }
                ctx.getAttributes(dn, LDAPUtils.NO_ATTRIBUTES);
                if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("LDAP.ExecutionTime", (System.currentTimeMillis() - startTime)));
                }
                if (ec.getStatistics() != null)
                {
                    ec.getStatistics().incrementNumReads();
                }
                return;
            }
            catch (NameNotFoundException e)
            {
                throw new NucleusObjectNotFoundException("Object not found", sm.getInternalObjectId());
            }
            catch (NamingException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            finally
            {
                mconn.release();
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            throw new NucleusUserException(Localiser.msg("LDAP.DatastoreID"));
        }
    }
}