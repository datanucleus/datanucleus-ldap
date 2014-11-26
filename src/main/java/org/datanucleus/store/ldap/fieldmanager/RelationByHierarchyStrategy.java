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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import javax.naming.directory.Attributes;
import javax.naming.ldap.LdapName;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.ldap.LDAPUtils.LocationInfo;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.ClassUtils;

import org.datanucleus.store.StoreManager;

/**
 * Mapping strategy for hierarchical mapping of relationships.
 */
public class RelationByHierarchyStrategy extends AbstractMappingStrategy
{
    protected int fieldNumber;

    protected StoreManager storeMgr;

    protected ClassLoaderResolver clr;

    protected AbstractClassMetaData effectiveClassMetaData;

    protected boolean isFieldHierarchicalMapped;

    protected boolean isFieldParentOfHierarchicalMapping;

    protected RelationByHierarchyStrategy(StoreManager storeMgr, ObjectProvider op, AbstractMemberMetaData mmd, Attributes attributes)
    {
        super(op, mmd, attributes);
        this.fieldNumber = mmd.getAbsoluteFieldNumber();
        this.storeMgr = storeMgr;
        this.clr = ec.getClassLoaderResolver();
        this.effectiveClassMetaData = LDAPUtils.getEffectiveClassMetaData(mmd, ec.getMetaDataManager());
        this.isFieldHierarchicalMapped = isChildOfHierarchicalMapping(mmd, ec.getMetaDataManager());
        this.isFieldParentOfHierarchicalMapping = isParentOfHierarchicalMapping(mmd, ec.getMetaDataManager());
    }

    public Object fetch()
    {
        if (isFieldHierarchicalMapped)
        {
            RelationType relType = mmd.getRelationType(clr);
            if (RelationType.isRelationSingleValued(relType))
            {
                // hierarchical mapped one-one relationships are always dependent!
                mmd.setDependent(true);
                return getHierarchicalMappedChild(mmd, op);
            }
            else if (relType == RelationType.ONE_TO_MANY_UNI || relType == RelationType.ONE_TO_MANY_BI)
            {
                if (mmd.hasCollection())
                {
                    Collection<Object> coll = getHierarchicalMappedChildren(mmd.getCollection().getElementType(), mmd.getMappedBy(), mmd, op);
                    return SCOUtils.wrapSCOField(op, fieldNumber, coll, false, false, true);
                }
            }
        }

        if (isFieldParentOfHierarchicalMapping)
        {
            // this field is used for hierarchical N-1 relation, load the parent
            LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, op, true);
            LocationInfo locationInfo = LDAPUtils.getLocationInfo(op.getClassMetaData());
            LdapName parentDn = LDAPUtils.getParentDistingueshedName(dn, locationInfo.suffix);

            Object value = null;
            if (locationInfo.dn == null || !locationInfo.dn.equals(parentDn))
            {
                value = LDAPUtils.getObjectByDN(storeMgr, ec, type, parentDn.toString());
            }
            return value;
        }

        // TODO Localise this
        throw new NucleusException("Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    private Object getHierarchicalMappedChild(AbstractMemberMetaData mmd, ObjectProvider sm)
    {
        Collection<Object> coll = getHierarchicalMappedChildren(mmd.getTypeName(), mmd.getMappedBy(), null, sm);
        if (coll.iterator().hasNext())
        {
            return coll.iterator().next();
        }

        return null;
    }

    private Collection<Object> getHierarchicalMappedChildren(String type, String mappedBy, AbstractMemberMetaData mmd, ObjectProvider sm)
    {
        Collection<Object> coll;

        Class collectionClass;
        if (mmd == null)
        {
            collectionClass = ArrayList.class;
        }
        else
        {
            collectionClass = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
        }

        try
        {
            coll = (Collection<Object>) collectionClass.newInstance();

            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            MetaDataManager metaDataMgr = sm.getExecutionContext().getMetaDataManager();
            AbstractClassMetaData childCmd = metaDataMgr.getMetaDataForClass(type, clr);
            LdapName base = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm, true);

            List<Object> objects = LDAPUtils.getObjectsOfCandidateType(storeMgr, ec, childCmd, base, null, true, false);
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

    public void insert(Object value)
    {
        if (value != null)
        {
            if (isFieldParentOfHierarchicalMapping)
            {
                // nothing to do
            }
            else if (isFieldHierarchicalMapped)
            {
                RelationType relType = mmd.getRelationType(clr);
                if (RelationType.isRelationSingleValued(relType))
                {
                    LDAPUtils.markForPersisting(value, ec);
                }
                else if (relType == RelationType.ONE_TO_MANY_UNI || relType == RelationType.ONE_TO_MANY_BI)
                {
                    if (mmd.hasCollection())
                    {
                        // 1-N (collection) relation
                        Collection c = (Collection) value;
                        for (Object pc : c)
                        {
                            LDAPUtils.markForPersisting(pc, ec);
                        }
                    }
                }
                else
                {
                    // TODO Localise this
                    throw new NucleusException("Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + 
                        " with relation type " + mmd.getRelationType(clr) + " is not supported for this datastore");
                }
            }
            else
            {
                // TODO Localise this
                throw new NucleusException("Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
            }
        }
    }

    public void update(Object value)
    {
        if (isFieldParentOfHierarchicalMapping)
        {
            if (value == null)
            {
                LDAPUtils.markForDeletion(op.getObject(), ec);
            }
        }
        else if (isFieldHierarchicalMapped)
        {
            if (value != null)
            {
                String mappedBy = mmd.getMappedBy();
                RelationType relType = mmd.getRelationType(clr);
                if (relType == RelationType.ONE_TO_ONE_UNI || relType == RelationType.MANY_TO_ONE_UNI)
                {
                    // Get the initial loaded object.
                    Object oldValue = getHierarchicalMappedChild(mmd, op);
                    if (oldValue != null)
                    {
                        ObjectProvider oldValueSM = ec.findObjectProvider(oldValue, false);
                        if (mustDelete(oldValueSM))
                        {
                            // delete only if the old value is still a child of this state manager
                            LDAPUtils.markForDeletion(oldValue, ec);
                        }
                    }

                    // if new value.sm != null
                    ObjectProvider valueSM = ec.findObjectProvider(value, false);
                    if (valueSM != null)
                    {
                        LdapName oldDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, valueSM, true);
                        LdapName newDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, valueSM);
                        LDAPUtils.markForRename(storeMgr, value, ec, oldDn, newDn);
                        LDAPUtils.unmarkForDeletion(value, ec);
                    }
                    LDAPUtils.markForPersisting(value, ec);
                    LDAPUtils.unmarkForDeletion(value, ec);
                }
                else if (relType == RelationType.ONE_TO_ONE_BI)
                {
                    LDAPUtils.markForPersisting(value, ec);
                    LDAPUtils.unmarkForDeletion(value, ec);
                }
                else if (relType == RelationType.ONE_TO_MANY_UNI || relType == RelationType.ONE_TO_MANY_BI)
                {
                    if (mmd.hasCollection())
                    {
                        Collection<Object> coll = (Collection<Object>) value;
                        // Get the collection with the initial loaded elements.
                        // It was saved by FetchFieldManager and is used to determine added and removed elements.
                        // This is just a quick and dirty implementation, could use backed SCO later.

                        Collection<Object> oldColl = getHierarchicalMappedChildren(mmd.getCollection().getElementType(), mmd.getMappedBy(), mmd, op);
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
                            LDAPUtils.markForPersisting(pc, ec);
                            LDAPUtils.unmarkForDeletion(pc, ec);
                        }

                        if (mmd.getRelationType(clr) == RelationType.ONE_TO_MANY_UNI)
                        {
                            toRemove.removeAll(coll);
                            for (Object pc : toRemove)
                            {
                                ObjectProvider pcSM = ec.findObjectProvider(pc, false);
                                if (mustDelete(pcSM))
                                {
                                    // delete only if the old value is still a child of this state manager
                                    // System.out.println("deleteMap.put(4): " + pc);
                                    LDAPUtils.markForDeletion(pc, ec);
                                }
                            }
                        }
                        else if (mappedBy != null)
                        {
                            toRemove.removeAll(coll);
                            for (Object pc : toRemove)
                            {
                                ObjectProvider pcSM = ec.findObjectProvider(pc, false);
                                if (mustDelete(pcSM, mappedBy))
                                {
                                    // System.out.println("deleteMap.put(5): " + pc);
                                    LDAPUtils.markForDeletion(pc, ec);
                                }
                            }
                        }
                    }
                }
                else
                {
                    // TODO Localise this
                    throw new NucleusException("Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd
                        .getTypeName() + " with relation type " + mmd.getRelationType(clr) + " is not supported for this datastore");
                }
            }
        }
        else
        {
            // TODO Localise this
            throw new NucleusException(
                    "Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
        }
    }

    /**
     * Checks if the given PC must be deleted.
     * @param pcSM the PC state manager
     * @return true if must delete
     */
    private boolean mustDelete(ObjectProvider pcSM)
    {
        LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, op, true);
        LdapName pcDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, pcSM, true);
        return pcDn.startsWith(dn);
    }

    /**
     * Checks if the given PC must be deleted.
     * @param pc The PC
     * @param mappedBy the mapped by field name
     * @return true if must delete
     */
    private boolean mustDelete(ObjectProvider pcSM, String mappedBy)
    {
        try
        {
            Object pc = pcSM.getObject();
            Class<? extends Object> clazz = pc.getClass();
            String methodName = ClassUtils.getJavaBeanGetterName(mappedBy, false);
            Method method = ClassUtils.getMethodForClass(clazz, methodName, null);
            Object fieldPC = method.invoke(pc);
            Object myPC = op.getObject();
            if (fieldPC == null || fieldPC == myPC)
            {
                return true;
            }
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
        }
        catch (InvocationTargetException e)
        {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean isChildOfHierarchicalMapping(AbstractMemberMetaData mmd, MetaDataManager mmgr)
    {
        String mappedBy = mmd.getMappedBy();
        if (mappedBy != null)
        {
            AbstractClassMetaData targetCmd = LDAPUtils.getEffectiveClassMetaData(mmd, mmgr);
            AbstractMemberMetaData mappedByMmd = targetCmd.getMetaDataForMember(mappedBy);
            if (mappedByMmd != null)
            {
                return isParentOfHierarchicalMapping(mappedByMmd, mmgr);
            }
        }

        return false;
    }

    /**
     * Checks if the given field is the parent of hierarchical mapping. This is the case if the fields name is equal to
     * the parent field name defined in distinguished name.
     * @param mmd the field meta data
     * @param mmgr Metadata manager
     * @return Whether is parent
     */
    public static boolean isParentOfHierarchicalMapping(AbstractMemberMetaData mmd, MetaDataManager mmgr)
    {
        AbstractClassMetaData cmd = mmd.getAbstractClassMetaData();
        LocationInfo locationInfo = LDAPUtils.getLocationInfo(cmd);
        if (locationInfo.parentFieldName != null)
        {
            return mmd.getName().equals(locationInfo.parentFieldName);
        }

        return false;
    }

    @Override
    public List<String> getAttributeNames()
    {
        return Collections.EMPTY_LIST;
    }
}
