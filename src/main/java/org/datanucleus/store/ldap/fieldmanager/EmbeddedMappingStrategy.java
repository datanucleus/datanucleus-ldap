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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.ldap.LdapName;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.types.SCOUtils;

/**
 * Mapping strategy for embedded objects.
 * NOTE THAT THE LOGIC IN THIS CLASS IS FLAWED. IT ASSUMES THAT "embeddedMetaData" IS PRESENT FOR NESTED CLASSES ALSO, YET IT ISN'T.
 * This needs rewriting to use the same structure used by ALL other store plugins, namely have a StoreEmbeddedFieldManager/FetchEmbeddedFieldManager and
 * from that you can manage nested embedded etc.
 */
public class EmbeddedMappingStrategy extends AbstractMappingStrategy
{
    protected AbstractClassMetaData effectiveClassMetaData;

    protected int fieldNumber;

    protected StoreManager storeMgr;

    protected ClassLoaderResolver clr;

    protected EmbeddedMappingStrategy(StoreManager storeMgr, DNStateManager sm, AbstractMemberMetaData mmd, Attributes attributes)
    {
        super(sm, mmd, attributes);
        this.fieldNumber = mmd.getAbsoluteFieldNumber();
        this.storeMgr = storeMgr;
        this.clr = ec.getClassLoaderResolver();
        this.effectiveClassMetaData = LDAPUtils.getEffectiveClassMetaData(mmd, ec.getMetaDataManager());
    }

    @Override
    public Object fetch()
    {
        RelationType relType = mmd.getRelationType(clr);
        if (relType == RelationType.ONE_TO_ONE_UNI || relType == RelationType.ONE_TO_ONE_BI)
        {
            Set<String> objectClasses = LDAPUtils.getObjectClassesForClass(effectiveClassMetaData);
            if (objectClasses.isEmpty())
            {
                // embedded into the current entry
                return fetchEmbedded();
            }

            // embedded as child-entry
            return fetchFromChild();
        }
        else if (relType == RelationType.ONE_TO_MANY_UNI || relType == RelationType.ONE_TO_MANY_BI)
        {
            if (mmd.hasCollection())
            {
                Class instanceType = mmd.getType();
                instanceType = org.datanucleus.store.types.SCOUtils.getContainerInstanceType(instanceType, mmd.getOrderMetaData() != null);
                Collection<Object> coll = fetchFromChildren(instanceType);
                return SCOUtils.wrapSCOField(sm, fieldNumber, coll, true);
            }
        }

        // TODO Localise this
        throw new NucleusException("Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    /**
     * Gets the object that is embedded within the current entry.
     * @return the object
     */
    private Object fetchEmbedded()
    {
        // use embedded meta data from field
        EmbeddedMetaData embeddedMetaData = mmd.getEmbeddedMetaData();

        // use field meta data from embedded meta data
        List<AbstractMemberMetaData> embeddedMmds = new ArrayList<AbstractMemberMetaData>(embeddedMetaData.getMemberMetaData());

        // TODO Provide the owner in this call
        DNStateManager embeddedSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, effectiveClassMetaData, null, -1, null);
        // TODO Why get SM just after creating it????
        embeddedSM = getEmbeddedStateManager(embeddedSM.getObject());

        return fetchMerge(embeddedSM, attributes, embeddedMmds, embeddedMetaData);
    }

    /**
     * Gets the object that is embedded from child entry under the current entry
     * @return the object
     */
    private Object fetchFromChild()
    {
        // use embedded meta data from field
        Collection<Object> children = fetchFromChildren(ArrayList.class, mmd, mmd.getEmbeddedMetaData());

        if (children.size() == 0)
        {
            return null;
        }
        else if (children.size() == 1)
        {
            return children.iterator().next();
        }
        else
        {
            throw new NucleusDataStoreException("Must be unique!");
        }
    }

    /**
     * Gets the objects that are embedded from child entries under the current entry.
     * @param collectionType the collection type
     * @return the objects
     */
    private Collection<Object> fetchFromChildren(Class collectionType)
    {
        // use embedded meta data from element
        return fetchFromChildren(collectionType, mmd, mmd.getElementMetaData().getEmbeddedMetaData());
    }

    private Collection<Object> fetchFromChildren(Class collectionType, AbstractMemberMetaData mmd, EmbeddedMetaData embeddedMetaData)
    {
        // use field meta data from class definition
        List<AbstractMemberMetaData> embeddedMmds = LDAPUtils.getAllMemberMetaData(effectiveClassMetaData);

        // search
        Collection<Object> coll = getCollectionInstance(collectionType);
        LdapName baseDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, sm);

        // search exact type
        Map<LdapName, Attributes> entries = LDAPUtils.getEntries(storeMgr, ec, effectiveClassMetaData, baseDn, null, false, false);
        for (Attributes embeddedAttrs : entries.values())
        {
            // TODO Populate the owner object in this call
            DNStateManager embeddedSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, effectiveClassMetaData, null, -1, null);
            // TODO Why get SM just after creating it????
            embeddedSM = getEmbeddedStateManager(embeddedSM.getObject());
            Object value = fetchMerge(embeddedSM, embeddedAttrs, embeddedMmds, embeddedMetaData);
            if (value != null)
            {
                coll.add(value);
            }
        }

        // search sub types
        String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(effectiveClassMetaData.getFullClassName(), true);
        if (subclassNames != null)
        {
            for (String subclassName : subclassNames)
            {
                AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(subclassName, clr);
                entries = LDAPUtils.getEntries(storeMgr, ec, cmd, baseDn, null, false, false);
                embeddedMmds = LDAPUtils.getAllMemberMetaData(cmd);
                for (Attributes embeddedAttrs : entries.values())
                {
                    // TODO Pass in owner to this call
                    DNStateManager embeddedSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, cmd, null, -1, null);
                    // TODO Why get SM just after creating it????
                    embeddedSM = getEmbeddedStateManager(embeddedSM.getObject());
                    Object value = fetchMerge(embeddedSM, embeddedAttrs, embeddedMmds, embeddedMetaData);
                    if (value != null)
                    {
                        coll.add(value);
                    }
                }
            }
        }
        
        return coll;
    }

    private Object fetchMerge(DNStateManager embeddedSM, Attributes embeddedAttrs, List<AbstractMemberMetaData> embeddedMmds, EmbeddedMetaData embeddedMetaData)
    {
        if (embeddedAttrs != null && embeddedMmds != null)
        {
            FetchFieldManager fetchFM = new FetchFieldManager(storeMgr, embeddedSM, embeddedAttrs);
            AbstractClassMetaData embeddedCmd = embeddedSM.getClassMetaData();
            String nullIndicatorColumn = embeddedMetaData.getNullIndicatorColumn();
            String nullIndicatorValue = embeddedMetaData.getNullIndicatorValue();

            // PK and owner first
            for (AbstractMemberMetaData embeddedMmd : embeddedMmds)
            {
                RelationType relType = embeddedMmd.getRelationType(clr);
                String fieldName = embeddedMmd.getName();
                int embFieldNum = embeddedCmd.getAbsolutePositionOfMember(fieldName);

                if (fieldName.equals(embeddedMetaData.getOwnerMember()))
                {
                    embeddedSM.replaceField(embFieldNum, sm.getObject());
                }
                else if (embeddedMmd.isPrimaryKey())
                {
                    if (relType == RelationType.NONE)
                    {
                        embeddedSM.replaceFields(new int[]{embFieldNum}, fetchFM);
                    }
                    else
                    {
                        AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, embeddedSM, embeddedMmd, embeddedAttrs);
                        embeddedSM.replaceField(embFieldNum, ms.fetch());
                    }
                }
            }

            // Then other fields
            for (AbstractMemberMetaData embeddedMmd : embeddedMmds)
            {
                String fieldName = embeddedMmd.getName();
                String attributeName = LDAPUtils.getAttributeNameForField(embeddedMmd);
                int embFieldNum = embeddedCmd.getAbsolutePositionOfMember(fieldName);

                if (!fieldName.equals(embeddedMetaData.getOwnerMember()) && !embeddedMmd.isPrimaryKey())
                {
                    // TODO If no mapping strategy then use FetchFieldManager (embedded)
                    AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, embeddedSM, embeddedMmd, embeddedAttrs);
                    Object embeddedValue = ms.fetch();

                    if (nullIndicatorColumn != null && attributeName.equals(nullIndicatorColumn))
                    {
                        if (embeddedValue == null)
                        {
                            return null;
                        }
                        if (nullIndicatorValue != null && embeddedValue.equals(nullIndicatorValue))
                        {
                            return null;
                        }
                    }

                    embeddedSM.replaceField(embFieldNum, embeddedValue);
                }
            }
            return embeddedSM.getObject();
        }

        return null;
    }

    private Collection<Object> getCollectionInstance(Class<?> collectionType)
    {
        try
        {
            return (Collection<Object>) collectionType.getDeclaredConstructor().newInstance();
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e)
        {
            throw new NucleusException("Error in trying to create object of type " + collectionType.getName(), e);
        }
    }

    private DNStateManager getEmbeddedStateManager(Object value)
    {
        return ec.findStateManagerForEmbedded(value, sm, mmd, null); // TODO Set last argument
    }

    @Override
    public void insert(Object value)
    {
        if (value == null)
        {
            return;
        }

        RelationType relationType = mmd.getRelationType(clr);
        if (relationType == RelationType.ONE_TO_ONE_UNI || relationType == RelationType.ONE_TO_ONE_BI)
        {
            Set<String> objectClasses = LDAPUtils.getObjectClassesForClass(effectiveClassMetaData);
            if (objectClasses.isEmpty())
            {
                // embedded into the current entry
                insertEmbedded(value);
            }
            else
            {
                // embedded as child-entry
                EmbeddedMetaData embeddedMetaData = mmd.getEmbeddedMetaData();
                insertAsChild(value, embeddedMetaData);
            }
        }
        else if (relationType == RelationType.ONE_TO_MANY_UNI || relationType == RelationType.ONE_TO_MANY_BI)
        {
            if (mmd.hasCollection())
            {
                // 1-N (collection) relation
                EmbeddedMetaData embeddedMetaData = mmd.getElementMetaData().getEmbeddedMetaData();
                Collection c = (Collection) value;
                for (Object pc : c)
                {
                    insertAsChild(pc, embeddedMetaData);
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

    private void insertEmbedded(Object value)
    {
        DNStateManager embeddedSM = getEmbeddedStateManager(value);
        EmbeddedMetaData embeddedMetaData = mmd.getEmbeddedMetaData();
        List<AbstractMemberMetaData> embeddedMmds = new ArrayList<AbstractMemberMetaData>(embeddedMetaData.getMemberMetaData());
        insertMerge(embeddedSM, attributes, embeddedMmds, embeddedMetaData);
    }

    private void insertAsChild(Object value, EmbeddedMetaData embeddedMetaData)
    {
        // merge fields
        DNStateManager embeddedSM = getEmbeddedStateManager(value);
        effectiveClassMetaData = embeddedSM.getClassMetaData();
        List<AbstractMemberMetaData> embeddedMmds = LDAPUtils.getAllMemberMetaData(effectiveClassMetaData);
        BasicAttributes embeddedAttributes = new BasicAttributes();

        // split embedded fields and non-embedded fields
        List<AbstractMemberMetaData> nonEmbeddedMmds = new ArrayList<AbstractMemberMetaData>();
        for (Iterator<AbstractMemberMetaData> it = embeddedMmds.iterator(); it.hasNext();)
        {
            AbstractMemberMetaData mmd = it.next();
            if (!LDAPUtils.isEmbeddedField(mmd))
            {
                nonEmbeddedMmds.add(mmd);
                it.remove();
            }
        }

        // 1st: non-embedded members
        insertMerge(embeddedSM, embeddedAttributes, nonEmbeddedMmds, embeddedMetaData);

        // add object classes
        BasicAttribute objectClass = new BasicAttribute("objectClass");
        Set<String> embeddedObjectClasses = LDAPUtils.getObjectClassesForClass(effectiveClassMetaData);
        for (String oc : embeddedObjectClasses)
        {
            objectClass.add(oc);
        }
        embeddedAttributes.put(objectClass);

        // create entry
        LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, embeddedSM);
        LDAPUtils.insert(storeMgr, dn, embeddedAttributes, ec);

        // 2nd: embedded members
        insertMerge(embeddedSM, embeddedAttributes, embeddedMmds, embeddedMetaData);
    }

    private void insertMerge(DNStateManager embeddedSM, Attributes embeddedAttributes, List<AbstractMemberMetaData> embeddedMmds, EmbeddedMetaData embeddedMetaData)
    {
        AbstractClassMetaData embeddedCmd = embeddedSM.getClassMetaData();

        // PK and owner first
//        StoreFieldManager storeFM = new StoreFieldManager(storeMgr, embeddedSM, embeddedAttributes, true);
        for (AbstractMemberMetaData embeddedMmd : embeddedMmds)
        {
            String fieldName = embeddedMmd.getName();
            int embFieldNum = embeddedCmd.getAbsolutePositionOfMember(fieldName);

            if (fieldName.equals(embeddedMetaData.getOwnerMember()))
            {
                embeddedSM.replaceField(embFieldNum, sm.getObject());
            }
            else if (embeddedMmd.isPrimaryKey())
            {
//                embeddedSM.provideFields(new int[] {i}, storeFM);
                // TODO If no mapping strategy then use StoreFieldManager (embedded)
                AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, embeddedSM, embeddedMmd, embeddedAttributes);
                ms.insert(embeddedSM.provideField(embFieldNum));
            }
        }

        for (AbstractMemberMetaData embeddedMmd : embeddedMmds)
        {
            String fieldName = embeddedMmd.getName();
            int embFieldNum = embeddedCmd.getAbsolutePositionOfMember(fieldName);

            if (!fieldName.equals(embeddedMetaData.getOwnerMember()) && !embeddedMmd.isPrimaryKey())
            {
//                embeddedSM.provideFields(new int[] {i}, storeFM);
                // TODO If no mapping strategy then use StoreFieldManager (embedded)
                AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, embeddedSM, embeddedMmd, embeddedAttributes);
                ms.insert(embeddedSM.provideField(embFieldNum));
            }
        }
    }

    @Override
    public void update(Object value)
    {
        RelationType relType = mmd.getRelationType(clr);
        if (relType == RelationType.ONE_TO_ONE_UNI || relType == RelationType.ONE_TO_ONE_BI)
        {
            Set<String> objectClasses = LDAPUtils.getObjectClassesForClass(effectiveClassMetaData);
            if (objectClasses.isEmpty())
            {
                // embedded into the current entry
                updateEmbedded(value);
            }
            else
            {
                // embedded as child-entry
                // fetch old value, need to replace it afterwards
                Object oldValue = fetchFromChild();
                sm.replaceField(fieldNumber, value);
                EmbeddedMetaData embeddedMetaData = mmd.getEmbeddedMetaData();
                if (value == null)
                {
                    if (oldValue != null)
                    {
                        // delete
                        deleteAsChild(oldValue, embeddedMetaData);
                    }
                }
                else
                {
                    if (oldValue == null)
                    {
                        // insert
                        insertAsChild(value, embeddedMetaData);
                    }
                    else
                    {
                        DNStateManager valueSM = getEmbeddedStateManager(value);
                        DNStateManager oldValueSM = getEmbeddedStateManager(oldValue);
                        LdapName valueDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, valueSM);
                        LdapName oldValueDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, oldValueSM);
                        if (!valueDn.equals(oldValueDn))
                        {
                            // delete old, insert new
                            deleteAsChild(oldValue, embeddedMetaData);
                            insertAsChild(value, embeddedMetaData);
                        }
                        else
                        {
                            // update
                            updateAsChild(value, embeddedMetaData);
                        }
                    }
                }
            }
        }
        else if (relType == RelationType.ONE_TO_MANY_UNI || relType == RelationType.ONE_TO_MANY_BI)
        {
            if (mmd.hasCollection())
            {
                // 1-N (collection) relation
                Collection<Object> coll = (Collection<Object>) value;
                LinkedHashMap<LdapName, Object> newMap = new LinkedHashMap<LdapName, Object>();
                for (Object newObject : coll)
                {
                    DNStateManager newObjectSM = getEmbeddedStateManager(newObject);
                    LdapName newObjectDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, newObjectSM);
                    newMap.put(newObjectDn, newObject);
                }

                EmbeddedMetaData embeddedMetaData = mmd.getElementMetaData().getEmbeddedMetaData();
                Class oldCollInstanceType = org.datanucleus.store.types.SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                Collection<Object> oldColl = fetchFromChildren(oldCollInstanceType, mmd, embeddedMetaData);
                LinkedHashMap<LdapName, Object> oldMap = new LinkedHashMap<LdapName, Object>();
                for (Object oldObject : oldColl)
                {
                    DNStateManager oldObjectSM = getEmbeddedStateManager(oldObject);
                    LdapName oldObjectDn = LDAPUtils.getDistinguishedNameForObject(storeMgr, oldObjectSM);
                    oldMap.put(oldObjectDn, oldObject);
                }

                Collection<Object> toInsert = null;
                Collection<Object> toUpdate = null;
                Collection<Object> toDelete = null;
                Class instanceType = mmd.getType();
                if (List.class.isAssignableFrom(instanceType))
                {
                    toInsert = new ArrayList<Object>();
                    toUpdate = new ArrayList<Object>();
                    toDelete = new ArrayList<Object>();
                }
                else
                {
                    toInsert = new HashSet<Object>();
                    toUpdate = new HashSet<Object>();
                    toDelete = new HashSet<Object>();
                }

                Iterator<LdapName> it = oldMap.keySet().iterator();
                while (it.hasNext())
                {
                    LdapName dn = it.next();
                    if (newMap.containsKey(dn))
                    {
                        toUpdate.add(newMap.get(dn));
                        newMap.remove(dn);
                        it.remove();
                    }
                }
                toInsert.addAll(newMap.values());
                toDelete.addAll(oldMap.values());

                for (Object pc : toInsert)
                {
                    insertAsChild(pc, embeddedMetaData);
                }
                for (Object pc : toUpdate)
                {
                    updateAsChild(pc, embeddedMetaData);
                }
                for (Object pc : toDelete)
                {
                    deleteAsChild(pc, embeddedMetaData);
                }
            }
        }
        else
        {
            // TODO Localise this
            throw new NucleusException("Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " with relation type " + mmd.getRelationType(clr) + 
                " is not supported for this datastore");
        }
    }

    private void updateAsChild(Object value, EmbeddedMetaData embeddedMetaData)
    {
        // update
        DNStateManager embeddedSM = ec.findStateManager(value);
        boolean insert = false;
        if (embeddedSM == null)
        {
            embeddedSM = getEmbeddedStateManager(value);
            insert = true;
        }

        effectiveClassMetaData = embeddedSM.getClassMetaData();

        // need to update dirty fields *and* embedded fields
        int[] dirtyFieldNumbers = embeddedSM.getDirtyFieldNumbers();
        List<AbstractMemberMetaData> dirtyAndEmbeddedMmds = LDAPUtils.getMemberMetaData(dirtyFieldNumbers, effectiveClassMetaData);
        List<AbstractMemberMetaData> allMmds = LDAPUtils.getAllMemberMetaData(effectiveClassMetaData);
        for (AbstractMemberMetaData mmd : allMmds)
        {
            if (LDAPUtils.isEmbeddedField(mmd) && !dirtyAndEmbeddedMmds.contains(mmd))
            {
                dirtyAndEmbeddedMmds.add(mmd);
            }
        }
        if (dirtyAndEmbeddedMmds.isEmpty())
        {
            return;
        }

        // merge fields into the BasicAttributes data structure, this also updates nested embedded objects recursively
        BasicAttributes embeddedAttributes = new BasicAttributes();
        updateMerge(embeddedSM, embeddedAttributes, dirtyAndEmbeddedMmds, embeddedMetaData, insert);

        // schedule update of fields
        if (embeddedAttributes.size() > 0)
        {
            LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, embeddedSM);
            LDAPUtils.update(storeMgr, dn, embeddedAttributes, ec);
        }
    }

    private void deleteAsChild(Object oldValue, EmbeddedMetaData embeddedMetaData)
    {
        DNStateManager embeddedSM = getEmbeddedStateManager(oldValue);
        LdapName dn = LDAPUtils.getDistinguishedNameForObject(storeMgr, embeddedSM);
        LDAPUtils.deleteRecursive(storeMgr, dn, ec);
    }

    private void updateEmbedded(Object value)
    {
        if (value == null)
        {
            // create an instance with empty fields, this will null-out all embedded fields
            // TODO Populate the owner object in this call
            DNStateManager embeddedSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, effectiveClassMetaData, null, -1, null);
            int[] allMemberPositions = embeddedSM.getClassMetaData().getAllMemberPositions();
            for (int i : allMemberPositions)
            {
                embeddedSM.makeDirty(i);
            }
            //get the dummy value
            value = embeddedSM.getObject();
        }

        DNStateManager embeddedSM = ec.findStateManager(value);
        boolean insert = false;
        if (embeddedSM == null)
        {
            embeddedSM = getEmbeddedStateManager(value);
            insert = true;
        }

        EmbeddedMetaData embeddedMetaData = mmd.getEmbeddedMetaData();
        List<AbstractMemberMetaData> embeddedMmds = new ArrayList<AbstractMemberMetaData>(embeddedMetaData.getMemberMetaData());
        updateMerge(embeddedSM, attributes, embeddedMmds, embeddedMetaData, insert);
    }

    private void updateMerge(DNStateManager embeddedSM, Attributes embeddedAttributes, List<AbstractMemberMetaData> embeddedMmds, EmbeddedMetaData embeddedMetaData, boolean insert)
    {
        AbstractClassMetaData embeddedCmd = embeddedSM.getClassMetaData();

        // PK and owner first
        for (AbstractMemberMetaData embeddedMmd : embeddedMmds)
        {
            String fieldName = embeddedMmd.getName();
            int i = embeddedCmd.getAbsolutePositionOfMember(fieldName);

            if (fieldName.equals(embeddedMetaData.getOwnerMember()))
            {
                embeddedSM.replaceField(i, sm.getObject());
            }
            else if (embeddedMmd.isPrimaryKey())
            {
                // TODO If no mapping strategy then use FetchFieldManager (embedded)
                AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, embeddedSM, embeddedMmd, embeddedAttributes);
                ms.update(embeddedSM.provideField(i));
            }
        }

        String[] dirtyFieldNames = embeddedSM.getDirtyFieldNames();
        List<String> dirtyFieldNameList = dirtyFieldNames != null ? Arrays.asList(dirtyFieldNames) : new ArrayList<String>();

        for (AbstractMemberMetaData embeddedMmd : embeddedMmds)
        {
            String fieldName = embeddedMmd.getName();
            if (!fieldName.equals(embeddedMetaData.getOwnerMember()) && !embeddedMmd.isPrimaryKey())
            {
                int i = embeddedCmd.getAbsolutePositionOfMember(fieldName);
                // TODO If no mapping strategy then use FetchFieldManager (embedded)
                AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, embeddedSM, embeddedMmd, embeddedAttributes);
                Object embeddedValue = embeddedSM.provideField(i);
                if (insert || LDAPUtils.isEmbeddedField(embeddedMmd) || dirtyFieldNameList.contains(fieldName))
                {
                    ms.update(embeddedValue);
                }
            }
        }
    }

    @Override
    public List<String> getAttributeNames()
    {
        // no results for interface or abstract
        Class c = ec.getClassLoaderResolver().classForName(effectiveClassMetaData.getFullClassName());
        if (c.isInterface() || Modifier.isAbstract(c.getModifiers()))
        {
            return Collections.EMPTY_LIST;
        }

        List<String> names = new ArrayList<String>();

        EmbeddedMetaData embeddedMetaData = null;
        RelationType relType = mmd.getRelationType(clr);
        if (relType == RelationType.ONE_TO_ONE_UNI || relType == RelationType.ONE_TO_ONE_BI)
        {
            embeddedMetaData = mmd.getEmbeddedMetaData();
        }
        else if (relType == RelationType.ONE_TO_MANY_UNI || relType == RelationType.ONE_TO_MANY_BI)
        {
            embeddedMetaData = mmd.getElementMetaData().getEmbeddedMetaData();
        }

        if (embeddedMetaData != null)
        {
            List<AbstractMemberMetaData> embeddedMmds = new ArrayList<AbstractMemberMetaData>(embeddedMetaData.getMemberMetaData());
            // TODO Populate the owner object in this call
            DNStateManager embeddedSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, effectiveClassMetaData, null, -1, null);
            for (AbstractMemberMetaData embeddedMmd : embeddedMmds)
            {
                // TODO If no mapping strategy then use FetchFieldManager (embedded)
                AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, embeddedSM, embeddedMmd, new BasicAttributes());
                if (ms != null)
                {
                    names.addAll(ms.getAttributeNames());
                }
            }
        }

        return names;
    }
}
