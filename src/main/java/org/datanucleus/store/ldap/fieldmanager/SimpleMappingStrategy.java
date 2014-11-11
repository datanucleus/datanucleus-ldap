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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Mapping strategy for simple types like primitives, Strings, wrappers of primitives, and objects with an
 * ObjectStringConverter.
 */
public class SimpleMappingStrategy extends AbstractMappingStrategy
{
    public SimpleMappingStrategy(ObjectProvider sm, AbstractMemberMetaData mmd, Attributes attributes)
    {
        super(sm, mmd, attributes);
    }

    public Object fetch()
    {
        // check primitives
        if (Boolean.TYPE.isAssignableFrom(type))
        {
            return fetchBooleanField();
        }
        if (Byte.TYPE.isAssignableFrom(type))
        {
            return fetchByteField();
        }
        if (Character.TYPE.isAssignableFrom(type))
        {
            return fetchCharField();
        }
        if (Double.TYPE.isAssignableFrom(type))
        {
            return fetchDoubleField();
        }
        if (Float.TYPE.isAssignableFrom(type))
        {
            return fetchFloatField();
        }
        if (Integer.TYPE.isAssignableFrom(type))
        {
            return fetchIntField();
        }
        if (Long.TYPE.isAssignableFrom(type))
        {
            return fetchLongField();
        }
        if (Short.TYPE.isAssignableFrom(type))
        {
            return fetchShortField();
        }

        // non-primitives could be null
        if (attr == null)
        {
            return null;
        }

        // check String
        if (String.class.isAssignableFrom(type))
        {
            return fetchStringField();
        }

        // check wrappers of primitives
        if (Boolean.class.isAssignableFrom(type))
        {
            return fetchBooleanField();
        }
        if (Byte.class.isAssignableFrom(type))
        {
            return fetchByteField();
        }
        if (Character.class.isAssignableFrom(type))
        {
            return fetchCharField();
        }
        if (Float.class.isAssignableFrom(type))
        {
            return fetchFloatField();
        }
        if (Double.class.isAssignableFrom(type))
        {
            return fetchDoubleField();
        }
        if (Short.class.isAssignableFrom(type))
        {
            return fetchShortField();
        }
        if (Integer.class.isAssignableFrom(type))
        {
            return fetchIntField();
        }
        if (Long.class.isAssignableFrom(type))
        {
            return fetchLongField();
        }

        // check String converter
        TypeConverter converter = null;
        if (Date.class.isAssignableFrom(type))
        {
            converter = new DateToGeneralizedTimeStringConverter();
        }
        else if (Calendar.class.isAssignableFrom(type))
        {
            converter = new CalendarToGeneralizedTimeStringConverter();
        }
        else
        {
            converter = ec.getTypeManager().getTypeConverterForType(type, String.class);
        }
        if (converter != null)
        {
            return converter.toMemberType(fetchStringField());
        }

        if (type.isEnum())
        {
            return fetchEnumField(type);
        }

        // TODO Localise this
        throw new NucleusException(
                "Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    private String fetchStringField()
    {
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

    protected Enum fetchEnumField(Class type)
    {
        String stringValue = fetchStringField();
        if (stringValue == null)
        {
            return null;
        }
        Enum value = Enum.valueOf(type, stringValue);
        return value;
    }

    private boolean fetchBooleanField()
    {
        if (attr == null)
        {
            return false;
        }
        try
        {
            return new Boolean(attr.get(0).toString()).booleanValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    private byte fetchByteField()
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return new Byte(attr.get(0).toString()).byteValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    private char fetchCharField()
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

    private double fetchDoubleField()
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return new Double(attr.get(0).toString()).doubleValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    private float fetchFloatField()
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return new Float(attr.get(0).toString()).floatValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    private int fetchIntField()
    {
        if (attr == null)
        {
            return 0;
        }
        try
        {
            return new Integer(attr.get(0).toString()).intValue();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    private long fetchLongField()
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

    private short fetchShortField()
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

    public void insert(Object value)
    {
        store(value);
    }

    public void update(Object value)
    {
        // non-primitives could be null
        if (value == null)
        {
            attributes.put(new BasicAttribute(name));
            return;
        }

        store(value);
    }

    private void store(Object value)
    {
        // check primitives
        if (Boolean.TYPE.isAssignableFrom(type))
        {
            storeBooleanField((Boolean) value);
        }
        else if (Byte.TYPE.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Character.TYPE.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Double.TYPE.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Float.TYPE.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Integer.TYPE.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Long.TYPE.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Short.TYPE.isAssignableFrom(type))
        {
            storeField(value);
        }

        // non-primitives could be null
        else if (value == null)
        {
            return;
        }

        // check String
        else if (String.class.isAssignableFrom(type))
        {
            storeField(value);
        }

        // check wrappers of primitives
        else if (Boolean.class.isAssignableFrom(type))
        {
            storeBooleanField((Boolean) value);
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Character.class.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Float.class.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Double.class.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Short.class.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            storeField(value);
        }
        else if (Long.class.isAssignableFrom(type))
        {
            storeField(value);
        }

        else
        {
            // check String converter
            TypeConverter converter = null;
            if (Date.class.isAssignableFrom(type))
            {
                converter = new DateToGeneralizedTimeStringConverter();
            }
            else if (Calendar.class.isAssignableFrom(type))
            {
                converter = new CalendarToGeneralizedTimeStringConverter();
            }
            else
            {
                converter = ec.getTypeManager().getTypeConverterForType(type, String.class);
            }
            if (converter != null)
            {
                attributes.put(new BasicAttribute(name, converter.toDatastoreType(value)));
            }
            else if (type.isEnum())
            {
                storeField(value);
            }
            else
            {
                // TODO Localise this
                throw new NucleusException(
                        "Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
            }
        }
    }

    private void storeBooleanField(boolean value)
    {
        // Apache Directory 1.5+ seems to require uppercase for booleans
        attributes.put(new BasicAttribute(name, ("" + value).toUpperCase()));
    }

    private void storeField(Object value)
    {
        String stringValue = "" + value;
        if (stringValue.length() == 0)
        {
            // this deletes existing value
            stringValue = null;
        }
        attributes.put(new BasicAttribute(name, stringValue));
    }

    @Override
    public List<String> getAttributeNames()
    {
        List<String> names = new ArrayList<String>();
        names.add(name);
        return names;
    }
}
