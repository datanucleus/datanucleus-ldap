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

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Basic class for all mapping strategies. A mapping strategy is used to map values to LDAP attributes and entries.
 */
public abstract class AbstractMappingStrategy
{
    protected ExecutionContext ec;

    /** ObjectProvider. */
    protected ObjectProvider op;

    /** The JNDI attributes. */
    protected Attributes attributes;

    /** The JNDI attribute for the given field. */
    protected Attribute attr;

    /** The field meta data for the given field. */
    protected AbstractMemberMetaData mmd;

    /** The type of the field meta data. */
    protected Class type;

    /** The LDAP attribute name of the field meta data. */
    protected String name;

    /**
     * Instantiates a new abstract mapping strategy.
     * @param op ObjectProvider
     * @param mmd Metadata for the member
     * @param attributes the attributes
     */
    protected AbstractMappingStrategy(ObjectProvider op, AbstractMemberMetaData mmd, Attributes attributes)
    {
        this.ec = op.getExecutionContext();
        this.op = op;
        this.mmd = mmd;
        this.attributes = attributes;
        this.type = this.mmd.getType();
        this.name = LDAPUtils.getAttributeNameForField(this.mmd);
        this.attr = attributes.get(this.name);
    }

    /**
     * Inserts the given value(s) into LDAP.
     * @param value the value(s)
     */
    public abstract void insert(Object value);

    /**
     * Updates the given value(s) in LDAP.
     * @param value the value(s
     */
    public abstract void update(Object value);

    /**
     * Fetches the value(s) from LDAP
     * @return the value(s)
     */
    public abstract Object fetch();

    /**
     * Gets the attribute names needed to fetch the field.
     * @return the attribute names
     */
    public abstract List<String> getAttributeNames();

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
        MetaDataManager mmgr = op.getExecutionContext().getMetaDataManager();

        // TODO Replace this ghastly check. mmd.getRelationType tells you if it is a relation or not! nothing more needed
        AbstractClassMetaData effectiveClassMetaData = LDAPUtils.getEffectiveClassMetaData(mmd, mmgr);
        if (effectiveClassMetaData == null)
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

            if (type.isPrimitive() || String.class.isAssignableFrom(type) || Number.class.isAssignableFrom(type) || 
                Character.class.isAssignableFrom(type) || Boolean.class.isAssignableFrom(type) || Date.class.isAssignableFrom(type) || 
                Calendar.class.isAssignableFrom(type) || type.isEnum())
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
                    return new SimpleMappingStrategy(op, mmd, attributes);
                }
            }

            TypeConverter converter = op.getExecutionContext().getTypeManager().getTypeConverterForType(type, String.class);
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
}