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
 * Mapping strategy for relationship mapping by attribute.
 */
public class RelationByAttributeStrategy extends AbstractMappingStrategy
{
    protected int fieldNumber;

    protected StoreManager storeMgr;

    protected ClassLoaderResolver clr;

    protected AbstractClassMetaData effectiveClassMetaData;

    protected RelationByAttributeMetaData mappingMetaData;

    protected RelationByAttributeStrategy(StoreManager storeMgr, ObjectProvider sm, AbstractMemberMetaData mmd, Attributes attributes)
    {
        super(sm, mmd, attributes);
        this.fieldNumber = mmd.getAbsoluteFieldNumber();
        this.storeMgr = storeMgr;
        this.clr = ec.getClassLoaderResolver();
        this.effectiveClassMetaData = LDAPUtils.getEffectiveClassMetaData(mmd, ec.getMetaDataManager());
        this.mappingMetaData = new RelationByAttributeMetaData(mmd, ec.getMetaDataManager());
    }

    public Object fetch()
    {
        String ownerAttributeName = mappingMetaData.getOwnerAttributeName();
        String joinAttributeName = mappingMetaData.getJoinAttributeName();
        String emptyValue = mappingMetaData.getEmptyValue();

        // which side is owner of the relation? <element> or not
        RelationType relationType = mmd.getRelationType(clr);
        if (mappingMetaData.getNonOwnerMMD() == mmd)
        {
            Object joinAttributeValue = null;
            try
            {
                joinAttributeValue = attributes.get(joinAttributeName).get();
            }
            catch (NamingException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO: check empty value
                return getAttributeMappedReference(effectiveClassMetaData, ownerAttributeName, joinAttributeValue, ec);
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                if (mmd.hasCollection())
                {
                    Collection<Object> coll = getAttributeMappedReferences(effectiveClassMetaData, mmd, ownerAttributeName,
                        joinAttributeValue, ec);
                    return op.wrapSCOField(fieldNumber, coll, false, false, true);
                }
            }

            throw new NucleusException(Localiser.msg("LDAP.RelationTypeNotSupported", mmd.getFullFieldName(),
                mmd.getRelationType(clr)));
        }

        // current object is owner of the relation
        if (RelationType.isRelationSingleValued(relationType))
        {
            // TODO: check empty value
            try
            {
                Object value = attr != null ? LDAPUtils.getObjectByAttribute(storeMgr, ec, mmd.getType(), joinAttributeName, (String) attr
                    .get(0), op.getExecutionContext().getMetaDataManager()) : null;
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
                        Object o = LDAPUtils.getObjectByAttribute(storeMgr, ec, elementType, joinAttributeName, value, op
                            .getExecutionContext().getMetaDataManager());
                        coll.add(o);
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

        throw new NucleusException(Localiser.msg("LDAP.Retrieve.RelationTypeNotSupported", mmd.getFullFieldName(), mmd.getRelationType(clr)));
    }

    public void insert(Object value)
    {
        if (value != null)
        {
            String ownerAttributeName = mappingMetaData.getOwnerAttributeName();
            String joinAttributeName = mappingMetaData.getJoinAttributeName();
            String emptyValue = mappingMetaData.getEmptyValue();

            // which side is owner of the relation? <element>/mapped-by or not
            RelationType relationType = mmd.getRelationType(clr);
            if (mappingMetaData.getNonOwnerMMD() == mmd)
            {
                Object joinAttributeValue = LDAPUtils.getAttributeValue(storeMgr, op, joinAttributeName);
                if (RelationType.isRelationSingleValued(relationType))
                {
                    addAttributeReference(value, ownerAttributeName, joinAttributeValue, emptyValue, ec);
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
                            addAttributeReference(pc, ownerAttributeName, joinAttributeValue, emptyValue, ec);
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
                    Object joinAttributeValue = LDAPUtils.getAttributeValue(storeMgr, pcSM, joinAttributeName);
                    BasicAttribute attr = new BasicAttribute(ownerAttributeName, joinAttributeValue);
                    addEmptyValue(emptyValue, attr);
                    attributes.put(attr);
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    if (mmd.hasCollection())
                    {
                        // 1-N (collection) relation
                        Collection c = (Collection) value;
                        BasicAttribute attr = new BasicAttribute(ownerAttributeName);
                        for (Object pc : c)
                        {
                            ObjectProvider pcSM = LDAPUtils.getObjectProviderForObject(pc, ec, true);
                            Object joinAttributeValue = LDAPUtils.getAttributeValue(storeMgr, pcSM, joinAttributeName);
                            attr.add(joinAttributeValue);
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
        String joinAttributeName = mappingMetaData.getJoinAttributeName();
        String emptyValue = mappingMetaData.getEmptyValue();

        // which side is owner of the relation? <element>/mapped-by or not
        RelationType relationType = mmd.getRelationType(clr);
        if (mappingMetaData.getNonOwnerMMD() == mmd)
        {
            // other object is owner of the relation
            Object joinAttributeValue = LDAPUtils.getAttributeValue(storeMgr, op, joinAttributeName);

            if (value != null)
            {
                if (RelationType.isRelationSingleValued(relationType))
                {
                    Object oldValue = getAttributeMappedReference(effectiveClassMetaData, ownerAttributeName, joinAttributeValue, ec);
                    if (!value.equals(oldValue))
                    {
                        LDAPUtils.markForPersisting(value, ec);
                        LDAPUtils.unmarkForDeletion(value, ec);
                        removeAttributeReference(oldValue, ownerAttributeName, joinAttributeValue, emptyValue, ec);
                        addAttributeReference(value, ownerAttributeName, joinAttributeValue, emptyValue, ec);
                    }
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    if (mmd.hasCollection())
                    {
                        Collection<Object> coll = (Collection<Object>) value;
                        Collection<Object> oldColl = getAttributeMappedReferences(effectiveClassMetaData, mmd, ownerAttributeName,
                            joinAttributeValue, ec);
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
                                addAttributeReference(pc, ownerAttributeName, joinAttributeValue, emptyValue, ec);
                                LDAPUtils.unmarkForDeletion(pc, ec);
                            }
                            toRemove.removeAll(coll);
                            for (Object pc : toRemove)
                            {
                                removeAttributeReference(pc, ownerAttributeName, joinAttributeValue, emptyValue, ec);
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
                Object oldValue = getAttributeMappedReference(effectiveClassMetaData, ownerAttributeName, joinAttributeValue, ec);
                removeAttributeReference(oldValue, ownerAttributeName, joinAttributeValue, emptyValue, ec);
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
                    // TODO: delete dependent
                    ObjectProvider smpc = LDAPUtils.getObjectProviderForObject(value, ec, true);
                    LDAPUtils.unmarkForDeletion(value, ec);
                    Object joinAttributeValue = LDAPUtils.getAttributeValue(storeMgr, smpc, joinAttributeName);
                    attributes.put(new BasicAttribute(ownerAttributeName, joinAttributeValue));
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    if (mmd.hasCollection())
                    {
                        Collection coll = (Collection) value;
                        // cascade-delete/dependent-element
                        if (mmd.getCollection().isDependentElement())
                        {
                            Collection<Object> attributeValues = LDAPUtils.getAttributeValuesFromLDAP(storeMgr, op, ownerAttributeName);
                            Collection<Object> oldColl = null;
                            Class instanceType = mmd.getType();
                            instanceType = SCOUtils.getContainerInstanceType(instanceType, mmd.getOrderMetaData() != null);
                            try
                            {
                                oldColl = (Collection<Object>) instanceType.newInstance();
                                Class elementType = clr.classForName(mmd.getCollection().getElementType());
                                for (Object object : attributeValues)
                                {
                                    String oldValue = (String) object;
                                    Object o = LDAPUtils.getObjectByAttribute(storeMgr, ec, elementType, joinAttributeName, oldValue, op
                                            .getExecutionContext().getMetaDataManager());
                                    oldColl.add(o);
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
                            Object joinAttributeValue = LDAPUtils.getAttributeValue(storeMgr, smpc, joinAttributeName);
                            attr.add(joinAttributeValue);
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

    private Object getAttributeMappedReference(AbstractClassMetaData cmd, String pcAttributeName,
            Object myAttributeValue, ExecutionContext om)
    {
        Collection<Object> coll = getAttributeMappedReferences(cmd, null, pcAttributeName, myAttributeValue, om);
        if (coll.iterator().hasNext())
        {
            Object value = coll.iterator().next();
            return value;
        }

        // no related object
        return null;
    }

    private Collection<Object> getAttributeMappedReferences(AbstractClassMetaData cmd, AbstractMemberMetaData mmd,
            String pcAttributeName, Object myAttributeValue, ExecutionContext om)
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

            String attributeFilter = "(" + pcAttributeName + "=" + myAttributeValue + ")";
            LdapName base = LDAPUtils.getSearchBase(cmd, om.getMetaDataManager());
            List<Object> objects = LDAPUtils.getObjectsOfCandidateType(storeMgr, om, cmd, base, attributeFilter, true, false);

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

    private void removeAttributeReference(Object fromObject, String attributeName, Object joinAttributeValue, String emptyValue,
            ExecutionContext om)
    {
        if (fromObject != null)
        {
            // delete reference from old object
            ObjectProvider fromSM = LDAPUtils.getObjectProviderForObject(fromObject, om, true);
            LdapName fromDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, fromSM);

            if (fromSM.getExecutionContext().getApiAdapter().isDeleted(fromObject))
            {
                // this object doesn't exist in LDAP any more
                return;
            }

            ManagedConnection mconn = storeMgr.getConnection(om);
            try
            {
                DirContext ctx = (DirContext) mconn.getConnection();
                Attributes attributes = ctx.getAttributes(fromDN, new String[]{attributeName});
                Attribute attribute = attributes.get(attributeName);
                if (attribute != null)
                {
                    boolean removed = attribute.remove(joinAttributeValue);
                    if (removed)
                    {
                        addEmptyValue(emptyValue, attribute);
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.DeleteAttributeReference", 
                                attributeName, joinAttributeValue, fromDN));
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.modifyAttributes",
                                fromDN, "REPLACE", attributes));
                        }
                        ctx.modifyAttributes(fromDN, DirContext.REPLACE_ATTRIBUTE, attributes);
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

    private void addAttributeReference(Object toObject, String attributeName, Object joinAttributeValue, String emptyValue,
            ExecutionContext om)
    {
        if (toObject != null)
        {
            // add reference to new object
            ObjectProvider toSM = LDAPUtils.getObjectProviderForObject(toObject, om, true);
            LdapName toDN = LDAPUtils.getDistinguishedNameForObject(storeMgr, toSM);

            if (toSM.isInserting())
            {
                // this object doesn't exist in LDAP yet
                return;
            }

            ManagedConnection mconn = storeMgr.getConnection(om);
            try
            {
                DirContext ctx = (DirContext) mconn.getConnection();
                Attributes attributes = ctx.getAttributes(toDN, new String[]{attributeName});
                Attribute attribute = attributes.get(attributeName);
                if (attribute == null)
                {
                    attribute = new BasicAttribute(attributeName);
                    attributes.put(attribute);
                }
                if (!attribute.contains(joinAttributeValue))
                {
                    attribute.add(joinAttributeValue);
                    removeEmptyValue(emptyValue, attribute);
                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.AddAttributeReference", attributeName,
                            joinAttributeValue, toDN));
                        NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("LDAP.JNDI.modifyAttributes", toDN, "REPLACE", attributes));
                    }
                    ctx.modifyAttributes(toDN, DirContext.REPLACE_ATTRIBUTE, attributes);
                }
            }
            catch (NamingException e)
            {
                System.out.println(toSM);
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
        if (emptyValue != null && attr != null)
        {
            attr.remove(emptyValue);
        }
    }

    @Override
    public List<String> getAttributeNames()
    {
        List<String> names = new ArrayList<String>();
        if (mappingMetaData.getNonOwnerMMD() == mmd)
        {
            names.add(mappingMetaData.getJoinAttributeName());
        }
        else
        {
            names.add(mappingMetaData.getOwnerAttributeName());
        }
        return names;
    }
}
