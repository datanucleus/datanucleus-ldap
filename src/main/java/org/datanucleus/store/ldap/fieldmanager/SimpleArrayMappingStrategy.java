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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.types.converters.ArrayConversionHelper;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Mapping strategy for arrays of simple types like primitives, Strings, wrappers of primitives, and objects with an
 * ObjectStringConverter.
 */
public class SimpleArrayMappingStrategy extends AbstractMappingStrategy
{
    public SimpleArrayMappingStrategy(ObjectProvider sm, AbstractMemberMetaData mmd, Attributes attributes)
    {
        super(sm, mmd, attributes);
        type = type.getComponentType();
    }

    public Object fetch()
    {
        if (attr == null)
        {
            return null;
        }
        if (Boolean.TYPE.isAssignableFrom(type))
        {
            return fetchBooleanArrayField();
        }
        else if (Byte.TYPE.isAssignableFrom(type))
        {
            return fetchByteArrayField();
        }
        else if (Character.TYPE.isAssignableFrom(type))
        {
            return fetchCharArrayField();
        }
        else if (Double.TYPE.isAssignableFrom(type))
        {
            return fetchDoubleArrayField();
        }
        else if (Float.TYPE.isAssignableFrom(type))
        {
            return fetchFloatArrayField();
        }
        else if (Integer.TYPE.isAssignableFrom(type))
        {
            return fetchIntArrayField();
        }
        else if (Long.TYPE.isAssignableFrom(type))
        {
            return fetchLongArrayField();
        }
        else if (Short.TYPE.isAssignableFrom(type))
        {
            return fetchShortArrayField();
        }
        else if (String.class.isAssignableFrom(type))
        {
            return fetchStringArrayField();
        }
        else if (Boolean.class.isAssignableFrom(type))
        {
            return fetchBooleanObjectArrayField();
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            return fetchByteObjectArrayField();
        }
        else if (Character.class.isAssignableFrom(type))
        {
            return fetchCharacterObjectArrayField();
        }
        else if (Double.class.isAssignableFrom(type))
        {
            return fetchDoubleObjectArrayField();
        }
        else if (Float.class.isAssignableFrom(type))
        {
            return fetchFloatObjectArrayField();
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            return fetchIntegerObjectArrayField();
        }
        else if (Long.class.isAssignableFrom(type))
        {
            return fetchLongObjectArrayField();
        }
        else if (Short.class.isAssignableFrom(type))
        {
            return fetchShortObjectArrayField();
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
            String[] stringValues = fetchStringArrayField();
            Object[] values = (Object[]) Array.newInstance(type, stringValues.length);
            for (int i = 0; i < stringValues.length; i++)
            {
                values[i] = converter.toMemberType(stringValues[i]);
            }
            return values;
        }

        if (type.isEnum())
        {
            return fetchEnumArrayField(type);
        }

        // TODO Localise this
        throw new NucleusException(
                "Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    protected boolean[] fetchBooleanArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getBooleanArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        boolean[] values = new boolean[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Boolean.valueOf(stringValues[i]).booleanValue();
        }
        return values;
    }

    protected Boolean[] fetchBooleanObjectArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getBooleanObjectArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        Boolean[] values = new Boolean[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Boolean.valueOf(stringValues[i]);
        }
        return values;
    }

    protected byte[] fetchByteArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            if (attr.size() == 0)
            {
                return null;
            }
            else if (attr.size() == 1)
            {
                try
                {
                    Object object = attr.get();
                    if (object instanceof byte[])
                    {
                        return (byte[]) object;
                    }
                    throw new NucleusException("Not a byte[]");
                }
                catch (NamingException e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }
            }
            else
            {
                throw new NucleusException("Can't fetch embedded byte[] from multi-valued attribute.");
            }
        }

        String[] stringValues = fetchStringArrayField();
        byte[] values = new byte[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Byte.valueOf(stringValues[i]).byteValue();
        }
        return values;
    }

    protected Byte[] fetchByteObjectArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getByteObjectArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        Byte[] values = new Byte[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Byte.valueOf(stringValues[i]);
        }
        return values;
    }

    protected char[] fetchCharArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getCharArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        char[] values = new char[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = stringValues[i].charAt(0);
        }
        return values;
    }

    protected Character[] fetchCharacterObjectArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getCharObjectArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        Character[] values = new Character[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Character.valueOf(stringValues[i].charAt(0));
        }
        return values;
    }

    protected double[] fetchDoubleArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getDoubleArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        double[] values = new double[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Double.valueOf(stringValues[i]).doubleValue();
        }
        return values;
    }

    protected Double[] fetchDoubleObjectArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getDoubleObjectArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        Double[] values = new Double[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Double.valueOf(stringValues[i]);
        }
        return values;
    }

    protected float[] fetchFloatArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getFloatArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        float[] values = new float[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Float.valueOf(stringValues[i]).floatValue();
        }
        return values;
    }

    protected Float[] fetchFloatObjectArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getFloatObjectArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        Float[] values = new Float[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Float.valueOf(stringValues[i]);
        }
        return values;
    }

    protected int[] fetchIntArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getIntArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        int[] values = new int[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Integer.valueOf(stringValues[i]).intValue();
        }
        return values;
    }

    protected Integer[] fetchIntegerObjectArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getIntObjectArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        Integer[] values = new Integer[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Integer.valueOf(stringValues[i]);
        }
        return values;
    }

    protected long[] fetchLongArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getLongArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        long[] values = new long[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Long.valueOf(stringValues[i]).longValue();
        }
        return values;
    }

    protected Long[] fetchLongObjectArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getLongObjectArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        Long[] values = new Long[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Long.valueOf(stringValues[i]);
        }
        return values;
    }

    protected short[] fetchShortArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getShortArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        short[] values = new short[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Short.valueOf(stringValues[i]).shortValue();
        }
        return values;
    }

    protected Short[] fetchShortObjectArrayField()
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            return ArrayConversionHelper.getShortObjectArrayFromByteArray(fetchByteArrayField());
        }

        String[] stringValues = fetchStringArrayField();
        Short[] values = new Short[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Short.valueOf(stringValues[i]);
        }
        return values;
    }

    protected Enum[] fetchEnumArrayField(Class type)
    {
        String[] stringValues = fetchStringArrayField();
        Enum[] values = (Enum[]) Array.newInstance(type, stringValues.length);
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Enum.valueOf(type, stringValues[i]);
        }
        return values;
    }

    protected String[] fetchStringArrayField()
    {
        try
        {
            String[] values = new String[attr.size()];
            for (int i = 0; i < attr.size(); i++)
            {
                if (attr.get(i) instanceof byte[])
                {
                    // this is to support passwords!!!
                    values[i] = new String((byte[]) attr.get(i), "UTF-8");
                }
                else
                {
                    values[i] = (String) attr.get(i);
                }
            }

            // ordering index
            if (mmd.getOrderMetaData() != null)
            {
                TreeMap<Integer, String> orderingMap = new TreeMap<Integer, String>();
                for (String value : values)
                {
                    int left = value.indexOf('{');
                    int right = value.indexOf('}');
                    if (left == 0 && right > 0)
                    {
                        try
                        {
                            String number = value.substring(left + 1, right);
                            Integer integer = Integer.valueOf(number);
                            value = value.substring(right + 1);
                            if (!orderingMap.containsKey(integer))
                            {
                                orderingMap.put(integer, value);
                            }
                            else
                            {
                                throw new NucleusDataStoreException("Ordering index must be unique: " + value);
                            }
                        }
                        catch (NumberFormatException e)
                        {
                            throw new NucleusDataStoreException("Can't parse ordering index: " + value);
                        }
                    }
                    else
                    {
                        throw new NucleusDataStoreException("No ordering index at value: " + value);
                    }
                }
                values = orderingMap.values().toArray(new String[orderingMap.size()]);
            }

            return values;
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

    public void insert(Object value)
    {
        if (value == null)
        {
            return;
        }

        store(value);
    }

    public void update(Object value)
    {
        if (value == null)
        {
            attributes.put(new BasicAttribute(name));
            return;
        }

        store(value);
    }

    protected void store(Object value)
    {
        if (Boolean.TYPE.isAssignableFrom(type))
        {
            storeBooleanArrayField((boolean[]) value);
        }
        else if (Byte.TYPE.isAssignableFrom(type))
        {
            storeByteArrayField((byte[]) value);
        }
        else if (Character.TYPE.isAssignableFrom(type))
        {
            storeCharArrayField((char[]) value);
        }
        else if (Double.TYPE.isAssignableFrom(type))
        {
            storeDoubleArrayField((double[]) value);
        }
        else if (Float.TYPE.isAssignableFrom(type))
        {
            storeFloatArrayField((float[]) value);
        }
        else if (Integer.TYPE.isAssignableFrom(type))
        {
            storeIntArrayField((int[]) value);
        }
        else if (Long.TYPE.isAssignableFrom(type))
        {
            storeLongArrayField((long[]) value);
        }
        else if (Short.TYPE.isAssignableFrom(type))
        {
            storeShortArrayField((short[]) value);
        }
        else if (String.class.isAssignableFrom(type))
        {
            storeObjectArrayField((String[]) value);
        }
        else if (Boolean.class.isAssignableFrom(type))
        {
            storeBooleanObjectArrayField((Boolean[]) value);
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            storeByteObjectArrayField((Byte[]) value);
        }
        else if (Character.class.isAssignableFrom(type))
        {
            storeCharacterObjectArrayField((Character[]) value);
        }
        else if (Double.class.isAssignableFrom(type))
        {
            storeDoubleObjectArrayField((Double[]) value);
        }
        else if (Float.class.isAssignableFrom(type))
        {
            storeFloatObjectArrayField((Float[]) value);
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            storeIntegerObjectArrayField((Integer[]) value);
        }
        else if (Long.class.isAssignableFrom(type))
        {
            storeLongObjectArrayField((Long[]) value);
        }
        else if (Short.class.isAssignableFrom(type))
        {
            storeShortObjectArrayField((Short[]) value);
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
                Object[] values = (Object[]) value;
                String[] stringValues = new String[values.length];
                for (int i = 0; i < values.length; i++)
                {
                    stringValues[i] = (String) converter.toDatastoreType(values[i]);
                }
                storeObjectArrayField(stringValues);
            }
            else if (type.isEnum())
            {
                storeObjectArrayField((Enum[]) value);
            }
            else
            {
                // TODO Localise this
                throw new NucleusException(
                        "Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
            }
        }
    }

    protected void storeBooleanArrayField(boolean[] values)
    {
        if (mmd.getJoinMetaData() == null)
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromBooleanArray(values));
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (boolean v : values)
            {
                // must be upper case (TRUE/FALSE)
                strings.add(("" + v).toUpperCase());
            }
            storeStringList(strings);
        }
    }

    protected void storeBooleanObjectArrayField(Boolean[] values)
    {
        if (mmd.getJoinMetaData() == null)
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromBooleanObjectArray(values));
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (boolean v : values)
            {
                // must be upper case (TRUE/FALSE)
                strings.add(("" + v).toUpperCase());
            }
            storeStringList(strings);
        }
    }

    protected void storeByteArrayField(byte[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            BasicAttribute attribute = new BasicAttribute(name, values);
            attributes.put(attribute);
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (byte v : values)
            {
                strings.add("" + v);
            }
            storeStringList(strings);
        }
    }

    protected void storeByteObjectArrayField(Byte[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromByteObjectArray(values));
        }
        else
        {
            storeObjectArrayField(values);
        }
    }

    protected void storeCharArrayField(char[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromCharArray(values));
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (char v : values)
            {
                strings.add("" + v);
            }
            storeStringList(strings);
        }
    }

    protected void storeCharacterObjectArrayField(Character[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromCharObjectArray(values));
        }
        else
        {
            storeObjectArrayField(values);
        }
    }

    protected void storeDoubleArrayField(double[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromDoubleArray(values));
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (double v : values)
            {
                strings.add("" + v);
            }
            storeStringList(strings);
        }
    }

    protected void storeDoubleObjectArrayField(Double[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromDoubleObjectArray(values));
        }
        else
        {
            storeObjectArrayField(values);
        }
    }

    protected void storeFloatArrayField(float[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromFloatArray(values));
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (float v : values)
            {
                strings.add("" + v);
            }
            storeStringList(strings);
        }
    }

    protected void storeFloatObjectArrayField(Float[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromFloatObjectArray(values));
        }
        else
        {
            storeObjectArrayField(values);
        }
    }

    protected void storeIntArrayField(int[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromIntArray(values));
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (int v : values)
            {
                strings.add("" + v);
            }
            storeStringList(strings);
        }
    }

    protected void storeIntegerObjectArrayField(Integer[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromIntObjectArray(values));
        }
        else
        {
            storeObjectArrayField(values);
        }
    }

    protected void storeLongArrayField(long[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromLongArray(values));
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (long v : values)
            {
                strings.add("" + v);
            }
            storeStringList(strings);
        }
    }

    protected void storeLongObjectArrayField(Long[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromLongObjectArray(values));
        }
        else
        {
            storeObjectArrayField(values);
        }
    }

    protected void storeShortArrayField(short[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromShortArray(values));
        }
        else
        {
            List<String> strings = new ArrayList<String>();
            for (short v : values)
            {
                strings.add("" + v);
            }
            storeStringList(strings);
        }
    }

    protected void storeShortObjectArrayField(Short[] values)
    {
        if (mmd.getJoinMetaData() == null) // This seems more like serialised to me
        {
            storeByteArrayField(ArrayConversionHelper.getByteArrayFromShortObjectArray(values));
        }
        else
        {
            storeObjectArrayField(values);
        }
    }

    /**
     * Stores the string representation of each array element as multi-valued attribute.
     * @param values the values
     */
    protected void storeObjectArrayField(Object[] values)
    {
        List<String> strings = new ArrayList<String>();
        for (Object v : values)
        {
            strings.add("" + v);
        }
        storeStringList(strings);
    }

    /**
     * Stores the each list element as multi-valued attribute.
     * @param values the values
     */
    protected void storeStringList(List<String> values)
    {
        BasicAttribute attribute = new BasicAttribute(name);
        for (int i = 0; i < values.size(); i++)
        {
            String value = values.get(i);
            if (mmd.getOrderMetaData() != null)
            {
                value = "{" + i + "}" + value;
            }
            attribute.add(value);
        }
        attributes.put(attribute);
    }

    @Override
    public List<String> getAttributeNames()
    {
        List<String> names = new ArrayList<String>();
        names.add(name);
        return names;
    }
}