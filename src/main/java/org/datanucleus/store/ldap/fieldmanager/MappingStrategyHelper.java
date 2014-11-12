/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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
     ...
 **********************************************************************/
package org.datanucleus.store.ldap.fieldmanager;

import java.util.Calendar;
import java.util.Date;

import javax.naming.directory.Attributes;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.ldap.LDAPStoreManager;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Helper for selecting and obtaining the mapping strategy for a member.
 */
public class MappingStrategyHelper
{
    /**
     * Finds the mapping strategy for the specified field of the state manager.
     * TODO We should remove ObjectProvider from XXXMappingStrategy since it is the same for all instances of a particular field, not per object.
     * @param storeMgr Store Manager
     * @param op state manager
     * @param mmd Metadata for the member
     * @param attributes the JNDI attributes, either to store or the fetched ones
     * @return the mapping strategy, null if now appropriate mapping strategy exists
     */
    public static AbstractMappingStrategy findMappingStrategy(StoreManager storeMgr, ObjectProvider op, AbstractMemberMetaData mmd, Attributes attributes)
    {
        MetaDataManager mmgr = storeMgr.getMetaDataManager();

        // TODO Replace this ghastly check. mmd.getRelationType tells you if it is a relation or not! nothing more needed
        ClassLoaderResolver clr = op.getExecutionContext().getClassLoaderResolver();
        RelationType relType = mmd.getRelationType(clr);
        if (relType == RelationType.NONE)
        {
            Class type = mmd.getType();
            boolean isArray = type.isArray();
            boolean isCollection = mmd.hasCollection();
            if (isArray)
            {
                type = type.getComponentType();
            }
            else if (isCollection)
            {
                type = op.getExecutionContext().getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            }

            if (isBasicTypeSupported(type))
            {
                if (isArray)
                {
                    return new SimpleArrayMappingStrategy(op, mmd, attributes);
                }
                else if (isCollection)
                {
                    return new SimpleCollectionMappingStrategy(op, mmd, attributes);
                }
                else
                {
                    // TODO Remove this. Only Embedded basic fields will come through here now
                    return new SimpleMappingStrategy(op, mmd, attributes);
                }
            }

            TypeConverter converter = storeMgr.getNucleusContext().getTypeManager().getTypeConverterForType(type, String.class);
            if (converter != null)
            {
                // TODO Use the converter in the mapping strategy since we took time to get it
                if (isArray)
                {
                    return new SimpleArrayMappingStrategy(op, mmd, attributes);
                }
                else if (isCollection)
                {
                    return new SimpleCollectionMappingStrategy(op, mmd, attributes);
                }
                else
                {
                    // TODO Remove this. Only Embedded basic fields will come through here now
                    return new SimpleMappingStrategy(op, mmd, attributes);
                }
            }

            return null;
        }

        if (LDAPUtils.isEmbeddedField(mmd))
        {
            // TODO See MetaDataUtils.isMemberEmbedded for a better embedded field test
            return new EmbeddedMappingStrategy(storeMgr, op, mmd, attributes);
        }

        if (mmd.hasExtension(LDAPStoreManager.MAPPING_STRATEGY_EXTENSON))
        {
            // User has specified the mapping-strategy explicitly via extension so use that
            String mappingStrategy = mmd.getValueForExtension(LDAPStoreManager.MAPPING_STRATEGY_EXTENSON);
            if (mappingStrategy != null)
            {
                if (mappingStrategy.equalsIgnoreCase("dn"))
                {
                    return new RelationByDnStrategy(storeMgr, op, mmd, attributes);
                }
                else if (mappingStrategy.equalsIgnoreCase("attribute"))
                {
                    return new RelationByAttributeStrategy(storeMgr, op, mmd, attributes);
                }
            }
        }

        // Fallback to deciding based on "join" and other such nonsense
        boolean isRelationByAttribute = RelationByAttributeMetaData.isRelationByAttribute(mmd, mmgr);
        if (isRelationByAttribute)
        {
            return new RelationByAttributeStrategy(storeMgr, op, mmd, attributes);
        }

        boolean isFieldHierarchicalMapped = RelationByHierarchyStrategy.isChildOfHierarchicalMapping(mmd, mmgr);
        boolean isFieldParentOfHierarchicalMapping = RelationByHierarchyStrategy.isParentOfHierarchicalMapping(mmd, mmgr);
        if (isFieldHierarchicalMapped || isFieldParentOfHierarchicalMapping)
        {
            return new RelationByHierarchyStrategy(storeMgr, op, mmd, attributes);
        }

        boolean isRelationByDn = RelationByDnMetaData.isRelationByDn(mmd, mmgr);
        if (isRelationByDn)
        {
            return new RelationByDnStrategy(storeMgr, op, mmd, attributes);
        }

        return null;
    }

    public static final boolean isBasicTypeSupported(Class type)
    {
        if (type == null)
        {
            return false;
        }
        return (type.isPrimitive() || String.class.isAssignableFrom(type) || Number.class.isAssignableFrom(type) || 
                Character.class.isAssignableFrom(type) || Boolean.class.isAssignableFrom(type) || Date.class.isAssignableFrom(type) || 
                Calendar.class.isAssignableFrom(type) || type.isEnum());
    }
}