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
    ...
 **********************************************************************/
package org.datanucleus.store.ldap.fieldmanager;

import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * FieldManager for retrieving field values from LDAP results.
 */
public class FetchFieldManager extends AbstractFieldManager
{
    ExecutionContext ec;
    DNStateManager sm;
    StoreManager storeMgr;
    Attributes result;

    // TODO Provide constructor that takes in ExecutionContext and AbstractClassMetaData so we can remove 
    // use of deprecated EC.findObjectUsingAID. This would mean that all XXXMappingStrategy take in ExecutionContext
    public FetchFieldManager(StoreManager storeMgr, DNStateManager sm, Attributes result)
    {
        this.ec = sm.getExecutionContext();
        this.sm = sm;
        this.storeMgr = storeMgr;
        this.result = result;
    }

    public Object fetchObjectField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        Attribute attr = getAttributeForField(fieldNumber);

        RelationType relType = mmd.getRelationType(clr);
        if (relType == RelationType.NONE)
        {
            if (mmd.hasCollection())
            {
                Collection coll = SimpleContainerHelper.fetchCollection(mmd, attr, ec.getTypeManager(), clr);
                return SCOUtils.wrapSCOField(sm, fieldNumber, coll, true);
            }
            else if (mmd.hasArray())
            {
                if (attr == null)
                {
                    return null;
                }
                return SimpleContainerHelper.fetchArray(mmd, attr, ec.getTypeManager());
            }
            else if (mmd.hasMap())
            {
                // TODO Support maps!
            }
            else
            {
                // Handle all basic types here
                if (attr == null)
                {
                    return null;
                }

                if (Boolean.class.isAssignableFrom(mmd.getType()))
                {
                    return fetchBooleanField(attr);
                }
                else if (Byte.class.isAssignableFrom(mmd.getType()))
                {
                    return fetchByteField(attr);
                }
                else if (Character.class.isAssignableFrom(mmd.getType()))
                {
                    return fetchCharField(attr);
                }
                else if (Double.class.isAssignableFrom(mmd.getType()))
                {
                    return fetchDoubleField(attr);
                }
                else if (Float.class.isAssignableFrom(mmd.getType()))
                {
                    return fetchFloatField(attr);
                }
                else if (Integer.class.isAssignableFrom(mmd.getType()))
                {
                    return fetchIntField(attr);
                }
                else if (Long.class.isAssignableFrom(mmd.getType()))
                {
                    return fetchLongField(attr);
                }
                else if (Short.class.isAssignableFrom(mmd.getType()))
                {
                    return fetchShortField(attr);
                }
                else if (mmd.getType().isEnum())
                {
                    // Retrieve as either ordinal or String
                    ColumnMetaData colmd = null;
                    if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                    {
                        colmd = mmd.getColumnMetaData()[0];
                    }
                    boolean useNumeric = MetaDataUtils.persistColumnAsNumeric(colmd);
                    if (useNumeric)
                    {
                        return mmd.getType().getEnumConstants()[fetchIntField(attr)];
                    }

                    return Enum.valueOf(mmd.getType(), fetchStringField(attr));
                }
                else
                {
                    // Support for TypeConverter
                    TypeConverter converter = null;
                    if (Date.class.isAssignableFrom(mmd.getType()))
                    {
                        converter = new DateToGeneralizedTimeStringConverter();
                    }
                    else if (Calendar.class.isAssignableFrom(mmd.getType()))
                    {
                        converter = new CalendarToGeneralizedTimeStringConverter();
                    }
                    else
                    {
                        converter = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    }
                    if (converter != null)
                    {
                        return converter.toMemberType(fetchStringField(attr));
                    }

                    throw new NucleusException("Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
                }
            }
        }

        AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, sm, mmd, result);
        if (ms != null)
        {
            return ms.fetch();
        }

        // TODO Localise this
        throw new NucleusException("Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    protected Attribute getAttributeForField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = LDAPUtils.getAttributeNameForField(mmd);
        return result.get(name);
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchBooleanField(attr);
    }

    public byte fetchByteField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchByteField(attr);
    }

    public char fetchCharField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchCharField(attr);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchDoubleField(attr);
    }

    public float fetchFloatField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchFloatField(attr);
    }

    public int fetchIntField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchIntField(attr);
    }

    public long fetchLongField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchLongField(attr);
    }

    public short fetchShortField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchShortField(attr);
    }

    public String fetchStringField(int fieldNumber)
    {
        Attribute attr = getAttributeForField(fieldNumber);
        return fetchStringField(attr);
    }

    protected boolean fetchBooleanField(Attribute attr)
    {
        if (attr == null)
        {
            return false;
        }
        try
        {
            return Boolean.valueOf(attr.get(0).toString()).booleanValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected byte fetchByteField(Attribute attr)
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return Byte.valueOf(attr.get(0).toString()).byteValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected char fetchCharField(Attribute attr)
    {
        if (attr == null)
        {
            return ' ';
        }
        try
        {
            return attr.get(0).toString().charAt(0);
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected double fetchDoubleField(Attribute attr)
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return Double.valueOf(attr.get(0).toString()).doubleValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected float fetchFloatField(Attribute attr)
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return Float.valueOf(attr.get(0).toString()).floatValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected int fetchIntField(Attribute attr)
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return Integer.valueOf(attr.get(0).toString()).intValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected long fetchLongField(Attribute attr)
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return Long.valueOf(attr.get(0).toString()).longValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected short fetchShortField(Attribute attr)
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return Short.valueOf(attr.get(0).toString()).shortValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected String fetchStringField(Attribute attr)
    {
        if (attr == null)
        {
            return null;
        }
        try
        {
            if (attr.get(0) instanceof byte[])
            {
                // this is to support passwords!!!
                return new String((byte[]) attr.get(0), "UTF-8");
            }
            return (String) attr.get(0);
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }
}