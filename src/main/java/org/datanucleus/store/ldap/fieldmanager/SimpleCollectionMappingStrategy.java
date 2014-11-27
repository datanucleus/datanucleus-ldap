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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

import javax.naming.directory.Attributes;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.JoinMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;

public class SimpleCollectionMappingStrategy extends SimpleArrayMappingStrategy
{
    public SimpleCollectionMappingStrategy(ObjectProvider sm, AbstractMemberMetaData mmd, Attributes attributes)
    {
        super(sm, mmd, attributes);
        type = sm.getExecutionContext().getClassLoaderResolver().classForName(mmd.getCollection().getElementType());

        // For now, each collection element is persisted as its own attribute value
        if (mmd.getJoinMetaData() == null)
        {
            // TODO Drop this hack. Using join as way of saying don't serialise
            mmd.setJoinMetaData(new JoinMetaData());
        }
    }

    public Object fetch()
    {
        Collection<Object> collection;
        Class instanceType = mmd.getType();
        instanceType = SCOUtils.getContainerInstanceType(instanceType, mmd.getOrderMetaData() != null);
        try
        {
            collection = (Collection<Object>) instanceType.newInstance();
        }
        catch (InstantiationException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }

        Object[] values = null;

        if (attr == null)
        {
            values = new Object[0];
        }
        else if (String.class.isAssignableFrom(type))
        {
            values = fetchStringArrayField();
        }
        else if (Boolean.class.isAssignableFrom(type))
        {
            values = fetchBooleanObjectArrayField();
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            values = fetchByteObjectArrayField();
        }
        else if (Character.class.isAssignableFrom(type))
        {
            values = fetchCharacterObjectArrayField();
        }
        else if (Double.class.isAssignableFrom(type))
        {
            values = fetchDoubleObjectArrayField();
        }
        else if (Float.class.isAssignableFrom(type))
        {
            values = fetchFloatObjectArrayField();
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            values = fetchIntegerObjectArrayField();
        }
        else if (Long.class.isAssignableFrom(type))
        {
            values = fetchLongObjectArrayField();
        }
        else if (Short.class.isAssignableFrom(type))
        {
            values = fetchShortObjectArrayField();
        }
        else
        {
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
                String[] stringValues = fetchStringArrayField();
                values = (Object[]) Array.newInstance(type, stringValues.length);
                for (int i = 0; i < stringValues.length; i++)
                {
                    values[i] = converter.toMemberType(stringValues[i]);
                }
            }
            else if (type.isEnum())
            {
                values = fetchEnumArrayField(type);
            }
        }

        if (values != null)
        {
            collection.addAll(Arrays.asList(values));

            int fieldNumber = op.isEmbedded() ? op.getClassMetaData().getAbsolutePositionOfMember(mmd.getName()) : mmd.getAbsoluteFieldNumber();
            op.setAssociatedValue("COLL" + fieldNumber, collection);
            return SCOUtils.wrapSCOField(op, fieldNumber, collection, true);
        }

        // TODO Localise this
        throw new NucleusException("Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    public void insert(Object value)
    {
        if (value == null)
        {
            return;
        }

        Collection<Object> valueCollection = (Collection<Object>) value;
        if (valueCollection.isEmpty())
        {
            return;
        }

        store(value);
    }

    public void update(Object value)
    {
        if (value == null)
        {
            return;
        }

        store(value);
    }

    protected void store(Object value)
    {
        Collection<Object> valueCollection = (Collection<Object>) value;
        Object[] values = valueCollection.toArray();

        if (String.class.isAssignableFrom(type))
        {
            storeObjectArrayField(values);
        }
        else if (Boolean.class.isAssignableFrom(type))
        {
            storeBooleanObjectArrayField(valueCollection.toArray(new Boolean[0]));
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            storeByteObjectArrayField(valueCollection.toArray(new Byte[0]));
        }
        else if (Character.class.isAssignableFrom(type))
        {
            storeCharacterObjectArrayField(valueCollection.toArray(new Character[0]));
        }
        else if (Double.class.isAssignableFrom(type))
        {
            storeDoubleObjectArrayField(valueCollection.toArray(new Double[0]));
        }
        else if (Float.class.isAssignableFrom(type))
        {
            storeFloatObjectArrayField(valueCollection.toArray(new Float[0]));
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            storeIntegerObjectArrayField(valueCollection.toArray(new Integer[0]));
        }
        else if (Long.class.isAssignableFrom(type))
        {
            storeLongObjectArrayField(valueCollection.toArray(new Long[0]));
        }
        else if (Short.class.isAssignableFrom(type))
        {
            storeShortObjectArrayField(valueCollection.toArray(new Short[0]));
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
                String[] stringValues = new String[values.length];
                for (int i = 0; i < values.length; i++)
                {
                    stringValues[i] = (String) converter.toDatastoreType(values[i]);
                }
                storeObjectArrayField(stringValues);
            }
            else if (type.isEnum())
            {
                storeObjectArrayField(values);
            }
            else
            {
                // TODO Localise this
                throw new NucleusException(
                        "Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
            }
        }
    }
}
