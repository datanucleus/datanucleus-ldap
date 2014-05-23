/**********************************************************************
Copyright (c) 2009 Stefan Seelmann and others. All rights reserved.
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
 ...
 ***********************************************************************/
package org.datanucleus.store.ldap.fieldmanager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.ldap.LdapName;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Mapping strategy for relationship mapping by DN.
 */
public class RelationByDnStrategy extends AbstractMappingStrategy
{
    protected int fieldNumber;

    protected StoreManager storeMgr;

    protected ClassLoaderResolver clr;

    protected AbstractClassMetaData effectiveClassMetaData;

    protected RelationByDnMetaData mappingMetaData;

    protected RelationByDnStrategy(StoreManager storeMgr, ObjectProvider sm, AbstractMemberMetaData mmd, 
            Attributes attributes)
    {
        super(sm, mmd, attributes);
        this.fieldNumber = mmd.getAbsoluteFieldNumber();
        this.storeMgr = storeMgr;
        this.clr = ec.getClassLoaderResolver();
        this.effectiveClassMetaData = LDAPUtils.getEffectiveClassMetaData(mmd, ec.getMetaDataManager());
        this.mappingMetaData = new RelationByDnMetaData(mmd, ec.getMetaDataManager());
    }

    public Object fetch()
    {
        String ownerAttributeName = mappingMetaData.getOwnerAttributeName();
        String emptyValue = mappingMetaData.getEmptyValue();

        // which side is owner of the relation? <element> or not
        RelationType relationType = mmd.getRelationType(clr);
        if (mappingMetaData.getNonOwnerMMD() == mmd)
        {
            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO: check empty value
                return getDnMappedReference(effectiveClassMetaData, ownerAttributeName, op);
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                if (mmd.hasCollection())
                {
                    Collection<Object> coll = getDnMappedReferences(effectiveClassMetaData, mmd, ownerAttributeName, op);
                    return op.wrapSCOField(fieldNumber, coll, false, false, true);
                }
            }

            throw new NucleusException(Localiser.msg("LDAP.Retrieve.RelationTypeNotSupported",
                mmd.getFullFieldName(), mmd.getRelationType(clr)));
        }
        else
        {
            // current object is owner of the relation
            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO: check empty value
                try
                {
                    Object value = attr != null ? LDAPUtils.getObjectByDN(storeMgr, ec, mmd.getType(), (String) attr.get(0)) : null;
                    return value;
                }
                catch (NamingException e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                if (mmd.hasCollection())
                {
                    Collection<Object> coll = null;
                    Class instanceType = mmd.getType();
                    instanceType = SCOUtils.getContainerInstanceType(instanceType, mmd.getOrderMetaData() != null);
                    try
                    {
                        coll = (Collection<Object>) instanceType.newInstance();
                        Class elementType = clr.classForName(mmd.getCollection().getElementType());
                        removeEmptyValue(emptyValue, attr);
                        for (int i = 0; attr != null && i < attr.size(); i++)
                        {
                            String value = (String) attr.get(i);
                            coll.add(LDAPUtils.getObjectByDN(storeMgr, ec, elementType, value));
                        }
                    }
                    catch (NamingException e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }
                    catch (Exception e)
                    {
                        throw new NucleusException("Error in trying to create object of type " + instanceType.getName(), e);
                    }
                    return op.wrapSCOField(fieldNumber, coll, false, false, true);
                }
            }

            throw new NucleusException(Localiser.msg("LDAP.Retrieve.RelationTypeNotSupported",
                mmd.getFullFieldName(), mmd.getRelationType(clr)));
        }
    }

    public void insert(Object value)
    {
        if (value != null)
        {
            String ownerAttributeName = mappingMetaData.getOwnerAttributeName();
            String emptyValue = mappingMetaData.getEmptyValue();

            // which side is owner of the relation? <element> or not
            RelationType relationType = mmd.getRelationType(clr);
            if (mappingMetaData.getNonOwnerMMD() == mmd)
            {
                LdapName myDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, op);
                if (RelationType.isRelationSingleValued(relationType))
                {
                    addDnReference(value, ownerAttributeName, myDN, emptyValue, ec);
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    if (mmd.hasCollection())
                    {
                        // 1-N (collection) relation
                        Collection c = (Collection) value;
                        for (Object pc : c)
                        {
                            LDAPUtils.unmarkForDeletion(pc, ec);
                            addDnReference(pc, ownerAttributeName, myDN, emptyValue, ec);
                        }
                    }
                }
                else
                {
                    throw new NucleusException(Localiser.msg("LDAP.Persist.RelationTypeNotSupported",
                        mmd.getFullFieldName(), mmd.getTypeName(), mmd.getRelationType(clr)));
                }
            }
            else
            {
                // current object is owner of the relation
                if (RelationType.isRelationSingleValued(relationType))
                {
                    ObjectProvider pcSM = LDAPUtils.getObjectProviderForObject(value, ec, true);
                    LdapName pcDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, pcSM);
                    // attributes.put(new BasicAttribute(name, pcDN.toString()));
                    BasicAttribute attr = new BasicAttribute(ownerAttributeName, pcDN.toString());
                    addEmptyValue(emptyValue, attr);
                    attributes.put(attr);
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    if (mmd.hasCollection())
                    {
                        // 1-N (collection) relation
                        Collection c = (Collection) value;
                        BasicAttribute attr = new BasicAttribute(name);
                        for (Object pc : c)
                        {
                            ObjectProvider pcSM = LDAPUtils.getObjectProviderForObject(pc, ec, true);
                            LdapName pcDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, pcSM);
                            attr.add(pcDN.toString());
                        }
                        addEmptyValue(emptyValue, attr);
                        if (attr.size() > 0)
                        {
                            attributes.put(attr);
                        }
                    }
                }
                else
                {
                    throw new NucleusException(Localiser.msg("LDAP.Persist.RelationTypeNotSupported",
                        mmd.getFullFieldName(), mmd.getTypeName(), mmd.getRelationType(clr)));
                }
            }
        }
    }

    public void update(Object value)
    {
        if (value != null)
        {
            LDAPUtils.unmarkForDeletion(op.getObject(), ec);
        }

        String ownerAttributeName = mappingMetaData.getOwnerAttributeName();
        String emptyValue = mappingMetaData.getEmptyValue();

        RelationType relationType = mmd.getRelationType(clr);
        if (mappingMetaData.getNonOwnerMMD() == mmd)
        {
            // other object is owner of the relation
            LdapName myDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, op);

            if (value != null)
            {
                if (RelationType.isRelationSingleValued(relationType))
                {
                    Object oldValue = getDnMappedReference(effectiveClassMetaData, ownerAttributeName, op);
                    if (!value.equals(oldValue))
                    {
                        LDAPUtils.markForPersisting(value, ec);
                        LDAPUtils.unmarkForDeletion(value, ec);
                        removeDnReference(oldValue, ownerAttributeName, myDN, emptyValue, ec);
                        addDnReference(value, ownerAttributeName, myDN, emptyValue, ec);
                    }
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    if (mmd.hasCollection())
                    {
                        Collection<Object> coll = (Collection<Object>) value;
                        Collection<Object> oldColl = getDnMappedReferences(effectiveClassMetaData, mmd, ownerAttributeName, op);
                        if (oldColl != null)
                        {
                            Collection<Object> toAdd = null;
                            Collection<Object> toRemove = null;
                            Class instanceType = mmd.getType();
                            if (List.class.isAssignableFrom(instanceType))
                            {
                                toAdd = new ArrayList<Object>(coll);
                                toRemove = new ArrayList<Object>(oldColl);
                            }
                            else
                            {
                                toAdd = new HashSet<Object>(coll);
                                toRemove = new HashSet<Object>(oldColl);
                            }

                            toAdd.removeAll(oldColl);
                            for (Object pc : toAdd)
                            {
                                addDnReference(pc, ownerAttributeName, myDN, emptyValue, ec);
                                LDAPUtils.unmarkForDeletion(pc, ec);
                            }
                            toRemove.removeAll(coll);
                            for (Object pc : toRemove)
                            {
                                removeDnReference(pc, ownerAttributeName, myDN, emptyValue, ec);
                                // cascade-delete/dependent-element
                                if (mmd.getCollection().isDependentElement())
                                {
                                    LDAPUtils.markForDeletion(pc, ec);
                                }
                            }
                        }
                        else
                        {
                            throw new NucleusDataStoreException("No old collection in SM " + op);
                        }
                    }
                }
                else
                {
                    throw new NucleusException(Localiser.msg("LDAP.Persist.RelationTypeNotSupported",
                        mmd.getFullFieldName(), mmd.getTypeName(), mmd.getRelationType(clr)));
                }
            }
            else
            {
                Object oldValue = getDnMappedReference(effectiveClassMetaData, ownerAttributeName, op);
                removeDnReference(oldValue, ownerAttributeName, myDN, emptyValue, ec);
            }
        }
        else
        {
            // current object is owner of the relation
            if (value != null)
            {
                if (RelationType.isRelationSingleValued(relationType))
                {
                    // TODO: check empty value
                    ObjectProvider smpc = LDAPUtils.getObjectProviderForObject(value, ec, true);
                    LDAPUtils.unmarkForDeletion(value, ec);
                    attributes.put(new BasicAttribute(ownerAttributeName, LDAPUtils.getDistinguishedNameForObject(storeMgr, smpc).toString()));
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    if (mmd.hasCollection())
                    {
                        Collection coll = (Collection) value;
                        // cascade-delete/dependent-element
                        if (mmd.getCollection().isDependentElement())
                        {
                            Collection<Object> oldColl = null;
                            Class instanceType = mmd.getType();
                            instanceType = SCOUtils.getContainerInstanceType(instanceType, mmd.getOrderMetaData() != null);
                            Collection<Object> attributeValues = LDAPUtils.getAttributeValuesFromLDAP(storeMgr, op, ownerAttributeName);
                            try
                            {
                                oldColl = (Collection<Object>) instanceType.newInstance();
                                Class elementType = clr.classForName(mmd.getCollection().getElementType());
                                for (Object object : attributeValues)
                                {
                                    String oldDn = (String) object;
                                    oldColl.add(LDAPUtils.getObjectByDN(storeMgr, ec, elementType, oldDn));
                                }
                            }
                            catch (Exception e)
                            {
                                throw new NucleusException("Error in trying to create object of type " + instanceType.getName(), e);
                            }

                            if (oldColl != null)
                            {
                                Collection<Object> toRemove = null;
                                if (List.class.isAssignableFrom(instanceType))
                                {
                                    toRemove = new ArrayList<Object>(oldColl);
                                }
                                else
                                {
                                    toRemove = new HashSet<Object>(oldColl);
                                }
                                toRemove.removeAll(coll);
                                for (Object pc : toRemove)
                                {
                                    LDAPUtils.markForDeletion(pc, ec);
                                }
                            }
                        }

                        BasicAttribute attr = new BasicAttribute(ownerAttributeName);
                        attributes.put(attr);
                        for (Object pc : coll)
                        {
                            LDAPUtils.unmarkForDeletion(pc, ec);
                            ObjectProvider smpc = LDAPUtils.getObjectProviderForObject(pc, ec, true);
                            attr.add(LDAPUtils.getDistinguishedNameForObject(storeMgr, smpc).toString());
                        }
                        addEmptyValue(emptyValue, attr);
                    }
                }
                else
                {
                    throw new NucleusException(Localiser.msg("LDAP.Persist.RelationTypeNotSupported",
                        mmd.getFullFieldName(), mmd.getTypeName(), mmd.getRelationType(clr)));
                }
            }
            else
            {
                BasicAttribute attr = new BasicAttribute(ownerAttributeName);
                addEmptyValue(emptyValue, attr);
                attributes.put(attr);
            }
        }
    }

    private Object getDnMappedReference(AbstractClassMetaData cmd, String pcAttributeName, ObjectProvider sm)
    {
        Collection<Object> coll = getDnMappedReferences(cmd, null, pcAttributeName, sm);
        if (coll.iterator().hasNext())
        {
            Object value = coll.iterator().next();
            return value;
        }
        else
        {
            // no related object
            return null;
        }
    }

    private Collection<Object> getDnMappedReferences(AbstractClassMetaData cmd, AbstractMemberMetaData mmd, String pcAttributeName,
        ObjectProvider sm)
    {
        Collection<Object> coll;

        Class collectionClass;
        if (mmd == null)
        {
            collectionClass = ArrayList.class;
        }
        else
        {
            collectionClass = mmd.getType();
            collectionClass = SCOUtils.getContainerInstanceType(collectionClass, mmd.getOrderMetaData() != null);
        }
        try
        {
            coll = (Collection<Object>) collectionClass.newInstance();

            ExecutionContext om = sm.getExecutionContext();
            LdapName myDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm);
            String dnFilter = "(" + pcAttributeName + "=" + myDN.toString() + ")";
            LdapName base = LDAPUtils.getSearchBase(cmd, sm.getExecutionContext().getMetaDataManager());
            List<Object> objects = LDAPUtils.getObjectsOfCandidateType(storeMgr, om, cmd, base, dnFilter, true, false);

            coll.addAll(objects);
        }
        catch (InstantiationException e)
        {
            throw new NucleusException("Error in trying to create object of type " + collectionClass.getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new NucleusException("Error in trying to create object of type " + collectionClass.getName(), e);
        }

        return coll;
    }

    private void removeDnReference(Object oldObject, String pcAttributeName, LdapName dn, String emptyValue, ExecutionContext om)
    {
        if (oldObject != null)
        {
            // delete reference from old object
            ObjectProvider oldPcSM = LDAPUtils.getObjectProviderForObject(oldObject, om, true);
            LdapName oldPcDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, oldPcSM);

            ManagedConnection mconn = storeMgr.getConnection(om);
            try
            {
                DirContext ctx = (DirContext) mconn.getConnection();
                Attributes pcAttributes = ctx.getAttributes(oldPcDN, new String[]{pcAttributeName});
                Attribute pcAttribute = pcAttributes.get(pcAttributeName);
                if (pcAttribute != null)
                {
                    boolean removed = pcAttribute.remove(dn.toString());
                    if (removed)
                    {
                        addEmptyValue(emptyValue, pcAttribute);
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.DeleteDnReference", oldPcDN, dn));
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.modifyAttributes", oldPcDN, "REPLACE",
                                pcAttributes));
                        }
                        ctx.modifyAttributes(oldPcDN, DirContext.REPLACE_ATTRIBUTE, pcAttributes);
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
    }

    private void addDnReference(Object newObject, String pcAttributeName, LdapName dn, String emptyValue, ExecutionContext om)
    {
        if (newObject != null)
        {
            // add reference to new object
            ObjectProvider newPcSM = LDAPUtils.getObjectProviderForObject(newObject, om, true);
            LdapName newPcDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, newPcSM);

            if (newPcSM.isInserting())
            {
                // this object doesn't exist in LDAP yet
                return;
            }

            ManagedConnection mconn = storeMgr.getConnection(om);
            try
            {
                DirContext ctx = (DirContext) mconn.getConnection();
                Attributes attributes = ctx.getAttributes(newPcDN, new String[]{pcAttributeName});
                Attribute attribute = attributes.get(pcAttributeName);
                if (attribute == null)
                {
                    attribute = new BasicAttribute(pcAttributeName);
                    attributes.put(attribute);
                }
                if(!attribute.contains(dn.toString()))
                {
                    attribute.add(dn.toString());
                    removeEmptyValue(emptyValue, attribute);
                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.AddDnReference", newPcDN, dn));
                        NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.modifyAttributes", newPcDN, "REPLACE", attributes));
                    }
                    ctx.modifyAttributes(newPcDN, DirContext.REPLACE_ATTRIBUTE, attributes);
                }
            }
            catch (NamingException e)
            {
                System.out.println(newPcSM);
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            finally
            {
                mconn.release();
            }
        }
    }

    private static void addEmptyValue(String emptyValue, Attribute attr)
    {
        if (attr != null && attr.size() == 0 && emptyValue != null)
        {
            attr.add(emptyValue);
        }
    }

    private static void removeEmptyValue(String emptyValue, Attribute attr)
    {
        try
        {
            // we must compare parsed LdapName objects rather then strings here
            if (emptyValue != null && attr != null)
            {
                LdapName emptyDn = new LdapName(emptyValue);
                for (int i = 0; i < attr.size(); i++)
                {
                    String value = (String) attr.get(i);
                    LdapName dn = new LdapName(value);
                    if (emptyDn.equals(dn))
                    {
                        attr.remove(i);
                        i--;
                    }
                }
            }
        }
        catch (NamingException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getAttributeNames()
    {
        List<String> names = new ArrayList<String>();
        if (mappingMetaData.getNonOwnerMMD() != mmd)
        {
            names.add(mappingMetaData.getOwnerAttributeName());
        }
        return names;
    }
}
