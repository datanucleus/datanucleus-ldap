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

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * FieldManager for inserting data into LDAP.
 */
public class StoreFieldManager extends AbstractFieldManager
{
    ObjectProvider op;
    StoreManager storeMgr;
    Attributes attributes;
    boolean insert;

    public StoreFieldManager(StoreManager storeMgr, ObjectProvider op, Attributes attrs, boolean insert)
    {
        this.op = op;
        this.storeMgr = storeMgr;
        this.attributes = attrs;
        this.insert = insert;
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = LDAPUtils.getAttributeNameForField(mmd);
        ClassLoaderResolver clr = op.getExecutionContext().getClassLoaderResolver();
        RelationType relType = mmd.getRelationType(clr);
        if (relType == RelationType.NONE)
        {
            if (mmd.hasCollection())
            {
                Collection<Object> valueCollection = (Collection<Object>) value;
                if (value == null)
                {
                    return;
                }
                else if (valueCollection.isEmpty() && insert)
                {
                    return;
                }
                else
                {
                    Attribute attr = SimpleContainerHelper.storeCollection(mmd, value, op.getExecutionContext().getTypeManager(), clr);
                    attributes.put(attr);
                }
                return;
            }
            else if (mmd.hasArray())
            {
                if (value == null)
                {
                    if (insert)
                    {
                        return;
                    }
                    attributes.put(new BasicAttribute(name));
                }
                else
                {
                    Attribute attr = SimpleContainerHelper.storeArray(mmd, value, op.getExecutionContext().getTypeManager());
                    attributes.put(attr);
                }
                return;
            }
            else if (mmd.hasMap())
            {
                // TODO Support maps!
            }
            else
            {
                if (value == null)
                {
                    if (insert)
                    {
                        return;
                    }
                    attributes.put(new BasicAttribute(name));
                    return;
                }

                if (Boolean.class.isAssignableFrom(mmd.getType()))
                {
                    storeBooleanField(fieldNumber, (Boolean) value);
                    return;
                }
                else if (Byte.class.isAssignableFrom(mmd.getType()))
                {
                    storeByteField(fieldNumber, (Byte)value);
                    return;
                }
                else if (Character.class.isAssignableFrom(mmd.getType()))
                {
                    storeCharField(fieldNumber, (Character)value);
                    return;
                }
                else if (Double.class.isAssignableFrom(mmd.getType()))
                {
                    storeDoubleField(fieldNumber, (Double)value);
                    return;
                }
                else if (Float.class.isAssignableFrom(mmd.getType()))
                {
                    storeFloatField(fieldNumber, (Float)value);
                    return;
                }
                else if (Integer.class.isAssignableFrom(mmd.getType()))
                {
                    storeIntField(fieldNumber, (Integer)value);
                    return;
                }
                else if (Long.class.isAssignableFrom(mmd.getType()))
                {
                    storeLongField(fieldNumber, (Long)value);
                    return;
                }
                else if (Short.class.isAssignableFrom(mmd.getType()))
                {
                    storeShortField(fieldNumber, (Short)value);
                    return;
                }
                else if (mmd.getType().isEnum())
                {
                    // Persist Enum as either String, or ordinal value
                    ColumnMetaData colmd = null;
                    if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                    {
                        colmd = mmd.getColumnMetaData()[0];
                    }
                    boolean useNumeric = MetaDataUtils.persistColumnAsNumeric(colmd);
                    storeStringValue(fieldNumber, "" + (useNumeric ? ((Enum)value).ordinal() : value.toString()));
                    return;
                }
                else
                {
                    // Support for TypeConverter
                    // Support user-specified type converter
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
                        converter = op.getExecutionContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    }
                    if (converter != null)
                    {
                        attributes.put(new BasicAttribute(name, converter.toDatastoreType(value)));
                        return;
                    }

                    throw new NucleusException("Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
                }
            }
        }

        AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, op, mmd, attributes);
        if (ms != null)
        {
            if (insert)
            {
                ms.insert(value);
            }
            else
            {
                ms.update(value);
            }
            return;
        }

        // TODO Localise this
        throw new NucleusException("Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = LDAPUtils.getAttributeNameForField(mmd);

        // Apache Directory 1.5+ seems to require uppercase for booleans
        attributes.put(new BasicAttribute(name, ("" + value).toUpperCase()));
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        storeStringValue(fieldNumber, "" + value);
    }

    public void storeCharField(int fieldNumber, char value)
    {
        storeStringValue(fieldNumber, "" + value);
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        storeStringValue(fieldNumber, "" + value);
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        storeStringValue(fieldNumber, "" + value);
    }

    public void storeIntField(int fieldNumber, int value)
    {
        storeStringValue(fieldNumber, "" + value);
    }

    public void storeLongField(int fieldNumber, long value)
    {
        storeStringValue(fieldNumber, "" + value);
    }

    public void storeShortField(int fieldNumber, short value)
    {
        storeStringValue(fieldNumber, "" + value);
    }

    public void storeStringField(int fieldNumber, String value)
    {
        storeStringValue(fieldNumber, value);
    }

    protected void storeStringValue(int fieldNumber, String stringValue)
    {
        if (stringValue != null && stringValue.length() == 0)
        {
            // this deletes existing value
            stringValue = null;
        }

        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = LDAPUtils.getAttributeNameForField(mmd);

        if (stringValue == null)
        {
            if (insert)
            {
                return;
            }

            attributes.put(new BasicAttribute(name));
            return;
        }

        // TODO We only ever put String values in here, maybe could use other types?
        attributes.put(new BasicAttribute(name, stringValue));
    }
}