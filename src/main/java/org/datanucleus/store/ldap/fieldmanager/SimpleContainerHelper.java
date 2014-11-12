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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.TypeConversionHelper;

/**
 * Methods to assist in the conversion between a container(collection/array) field (of non-persistable object) and an Attribute, and vice-versa.
 * A collection/array of non-persistable objects can be stored either as an Attribute with multiple String values (when there is "join" metadata specified), or
 * as an Attribute with a byte[]. 
 * TODO Change the determining factor from "join" to "serialized" as true maybe.
 */
public class SimpleContainerHelper
{
    public static Collection fetchCollection(AbstractMemberMetaData mmd, Attribute attr, TypeManager typeMgr, ClassLoaderResolver clr)
    {
        // With collections we always store as attribute with multiple values
        boolean singleAttribute = false;

        Class type = clr.classForName(mmd.getCollection().getElementType());
        Object[] values = null;

        if (attr == null)
        {
            values = new Object[0];
        }
        else if (String.class.isAssignableFrom(type))
        {
            values = fetchStringArrayField(attr, singleAttribute, mmd);
        }
        else if (Boolean.class.isAssignableFrom(type))
        {
            values = fetchBooleanObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            values = fetchByteObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Character.class.isAssignableFrom(type))
        {
            values = fetchCharacterObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Double.class.isAssignableFrom(type))
        {
            values = fetchDoubleObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Float.class.isAssignableFrom(type))
        {
            values = fetchFloatObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            values = fetchIntegerObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Long.class.isAssignableFrom(type))
        {
            values = fetchLongObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Short.class.isAssignableFrom(type))
        {
            values = fetchShortObjectArrayField(attr, singleAttribute, mmd);
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
                converter = typeMgr.getTypeConverterForType(type, String.class);
            }
            if (converter != null)
            {
                String[] stringValues = fetchStringArrayField(attr, singleAttribute, mmd);
                values = (Object[]) Array.newInstance(type, stringValues.length);
                for (int i = 0; i < stringValues.length; i++)
                {
                    values[i] = converter.toMemberType(stringValues[i]);
                }
            }
            else if (type.isEnum())
            {
                values = fetchEnumArrayField(attr, singleAttribute, mmd, type);
            }
        }

        if (values != null)
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

            collection.addAll(Arrays.asList(values));

            return collection;
        }

        // TODO Localise this
        throw new NucleusException("Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    /**
     * Method to retrieve the value for the specified array field from the LDAP Attribute.
     * @param mmd Metadata for the member (field/property).
     * @param attr The LDAP Attribute
     * @param typeMgr TypeManager
     * @return The value for the field
     */
    public static Object fetchArray(AbstractMemberMetaData mmd, Attribute attr, TypeManager typeMgr)
    {
        boolean singleAttribute = (mmd.getJoinMetaData() == null);
        Class type = mmd.getType().getComponentType();

        if (attr == null)
        {
            return null;
        }

        if (Boolean.TYPE.isAssignableFrom(type))
        {
            return fetchBooleanArrayField(attr, singleAttribute, mmd);
        }
        else if (Byte.TYPE.isAssignableFrom(type))
        {
            return fetchByteArrayField(attr, singleAttribute, mmd);
        }
        else if (Character.TYPE.isAssignableFrom(type))
        {
            return fetchCharArrayField(attr, singleAttribute, mmd);
        }
        else if (Double.TYPE.isAssignableFrom(type))
        {
            return fetchDoubleArrayField(attr, singleAttribute, mmd);
        }
        else if (Float.TYPE.isAssignableFrom(type))
        {
            return fetchFloatArrayField(attr, singleAttribute, mmd);
        }
        else if (Integer.TYPE.isAssignableFrom(type))
        {
            return fetchIntArrayField(attr, singleAttribute, mmd);
        }
        else if (Long.TYPE.isAssignableFrom(type))
        {
            return fetchLongArrayField(attr, singleAttribute, mmd);
        }
        else if (Short.TYPE.isAssignableFrom(type))
        {
            return fetchShortArrayField(attr, singleAttribute, mmd);
        }
        else if (String.class.isAssignableFrom(type))
        {
            return fetchStringArrayField(attr, singleAttribute, mmd);
        }
        else if (Boolean.class.isAssignableFrom(type))
        {
            return fetchBooleanObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            return fetchByteObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Character.class.isAssignableFrom(type))
        {
            return fetchCharacterObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Double.class.isAssignableFrom(type))
        {
            return fetchDoubleObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Float.class.isAssignableFrom(type))
        {
            return fetchFloatObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            return fetchIntegerObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Long.class.isAssignableFrom(type))
        {
            return fetchLongObjectArrayField(attr, singleAttribute, mmd);
        }
        else if (Short.class.isAssignableFrom(type))
        {
            return fetchShortObjectArrayField(attr, singleAttribute, mmd);
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
            converter = typeMgr.getTypeConverterForType(type, String.class);
        }
        if (converter != null)
        {
            String[] stringValues = fetchStringArrayField(attr, singleAttribute, mmd);
            Object[] values = (Object[]) Array.newInstance(type, stringValues.length);
            for (int i = 0; i < stringValues.length; i++)
            {
                values[i] = converter.toMemberType(stringValues[i]);
            }
            return values;
        }

        if (type.isEnum())
        {
            return fetchEnumArrayField(attr, singleAttribute, mmd, type);
        }

        // TODO Localise this
        throw new NucleusException("Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    protected static boolean[] fetchBooleanArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getBooleanArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        boolean[] values = new boolean[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Boolean(stringValues[i]).booleanValue();
        }
        return values;
    }

    protected static Boolean[] fetchBooleanObjectArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getBooleanObjectArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        Boolean[] values = new Boolean[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Boolean(stringValues[i]);
        }
        return values;
    }

    protected static Byte[] fetchByteObjectArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getByteObjectArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        Byte[] values = new Byte[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Byte(stringValues[i]);
        }
        return values;
    }

    protected static char[] fetchCharArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getCharArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        char[] values = new char[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = stringValues[i].charAt(0);
        }
        return values;
    }

    protected static Character[] fetchCharacterObjectArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getCharObjectArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        Character[] values = new Character[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Character(stringValues[i].charAt(0));
        }
        return values;
    }

    protected static double[] fetchDoubleArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getDoubleArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        double[] values = new double[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Double(stringValues[i]).doubleValue();
        }
        return values;
    }

    protected static Double[] fetchDoubleObjectArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getDoubleObjectArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        Double[] values = new Double[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Double(stringValues[i]);
        }
        return values;
    }

    protected static float[] fetchFloatArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getFloatArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        float[] values = new float[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Float(stringValues[i]).floatValue();
        }
        return values;
    }

    protected static Float[] fetchFloatObjectArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getFloatObjectArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        Float[] values = new Float[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Float(stringValues[i]);
        }
        return values;
    }

    protected static int[] fetchIntArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getIntArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        int[] values = new int[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Integer(stringValues[i]).intValue();
        }
        return values;
    }

    protected static Integer[] fetchIntegerObjectArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getIntObjectArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        Integer[] values = new Integer[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Integer(stringValues[i]);
        }
        return values;
    }

    protected static long[] fetchLongArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getLongArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        long[] values = new long[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Long.valueOf(stringValues[i]).longValue();
        }
        return values;
    }

    protected static Long[] fetchLongObjectArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getLongObjectArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        Long[] values = new Long[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Long.valueOf(stringValues[i]);
        }
        return values;
    }

    protected static short[] fetchShortArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getShortArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        short[] values = new short[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Short.valueOf(stringValues[i]).shortValue();
        }
        return values;
    }

    protected static Short[] fetchShortObjectArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = fetchByteArrayField(attr, true, mmd);
            return TypeConversionHelper.getShortObjectArrayFromByteArray(bytes);
        }

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        Short[] values = new Short[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = Short.valueOf(stringValues[i]);
        }
        return values;
    }

    protected static Enum[] fetchEnumArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd, Class type)
    {
        // TODO Support single attribute?
        String[] stringValues = fetchStringArrayField(attr, singleAttribute, mmd);
        Enum[] values = (Enum[]) Array.newInstance(type, stringValues.length);
        for (int i = 0; i < stringValues.length; i++)
        {
            // TODO Support storage as ordinal
            values[i] = Enum.valueOf(type, stringValues[i]);
        }
        return values;
    }

    protected static byte[] fetchByteArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
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

        String[] stringValues = fetchStringArrayField(attr, false, mmd);
        byte[] values = new byte[stringValues.length];
        for (int i = 0; i < stringValues.length; i++)
        {
            values[i] = new Byte(stringValues[i]).byteValue();
        }
        return values;
    }

    protected static String[] fetchStringArrayField(Attribute attr, boolean singleAttribute, AbstractMemberMetaData mmd)
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
                            Integer integer = new Integer(number);
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

    /**
     * Method to return the LDAP Attribute for the provided value for the specified collection field.
     * @param mmd Metadata for the member (field/property).
     * @param value The value to store
     * @param typeMgr TypeManager
     * @return The Attribute to store in LDAP
     */
    public static Attribute storeCollection(AbstractMemberMetaData mmd, Object value, TypeManager typeMgr, ClassLoaderResolver clr)
    {
        // With collections we always store as attribute with multiple values
        boolean singleAttribute = false;

        Class type = clr.classForName(mmd.getCollection().getElementType());
        Collection<Object> valueCollection = (Collection<Object>) value;
        Object[] values = valueCollection.toArray();

        if (String.class.isAssignableFrom(type))
        {
            return storeObjectArrayField(values, singleAttribute, mmd);
        }
        else if (Boolean.class.isAssignableFrom(type))
        {
            return storeBooleanObjectArrayField(valueCollection.toArray(new Boolean[0]), singleAttribute, mmd);
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            return storeByteObjectArrayField(valueCollection.toArray(new Byte[0]), singleAttribute, mmd);
        }
        else if (Character.class.isAssignableFrom(type))
        {
            return storeCharacterObjectArrayField(valueCollection.toArray(new Character[0]), singleAttribute, mmd);
        }
        else if (Double.class.isAssignableFrom(type))
        {
            return storeDoubleObjectArrayField(valueCollection.toArray(new Double[0]), singleAttribute, mmd);
        }
        else if (Float.class.isAssignableFrom(type))
        {
            return storeFloatObjectArrayField(valueCollection.toArray(new Float[0]), singleAttribute, mmd);
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            return storeIntegerObjectArrayField(valueCollection.toArray(new Integer[0]), singleAttribute, mmd);
        }
        else if (Long.class.isAssignableFrom(type))
        {
            return storeLongObjectArrayField(valueCollection.toArray(new Long[0]), singleAttribute, mmd);
        }
        else if (Short.class.isAssignableFrom(type))
        {
            return storeShortObjectArrayField(valueCollection.toArray(new Short[0]), singleAttribute, mmd);
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
                converter = typeMgr.getTypeConverterForType(type, String.class);
            }
            if (converter != null)
            {
                String[] stringValues = new String[values.length];
                for (int i = 0; i < values.length; i++)
                {
                    stringValues[i] = (String) converter.toDatastoreType(values[i]);
                }
                return storeObjectArrayField(stringValues, singleAttribute, mmd);
            }
            else if (type.isEnum())
            {
                return storeObjectArrayField(values, singleAttribute, mmd);
            }
            else
            {
                // TODO Localise this
                throw new NucleusException("Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
            }
        }
    }

    /**
     * Method to return the LDAP Attribute for the provided value for the specified array field.
     * @param mmd Metadata for the member (field/property).
     * @param value The value to store
     * @param typeMgr TypeManager
     * @return The Attribute to store in LDAP
     */
    public static Attribute storeArray(AbstractMemberMetaData mmd, Object value, TypeManager typeMgr)
    {
        boolean singleAttribute = (mmd.getJoinMetaData() == null);
        Class type = mmd.getType().getComponentType();

        if (Boolean.TYPE.isAssignableFrom(type))
        {
            return storeBooleanArrayField((boolean[]) value, singleAttribute, mmd);
        }
        else if (Byte.TYPE.isAssignableFrom(type))
        {
            return storeByteArrayField((byte[]) value, singleAttribute, mmd);
        }
        else if (Character.TYPE.isAssignableFrom(type))
        {
            return storeCharArrayField((char[]) value, singleAttribute, mmd);
        }
        else if (Double.TYPE.isAssignableFrom(type))
        {
            return storeDoubleArrayField((double[]) value, singleAttribute, mmd);
        }
        else if (Float.TYPE.isAssignableFrom(type))
        {
            return storeFloatArrayField((float[]) value, singleAttribute, mmd);
        }
        else if (Integer.TYPE.isAssignableFrom(type))
        {
            return storeIntArrayField((int[]) value, singleAttribute, mmd);
        }
        else if (Long.TYPE.isAssignableFrom(type))
        {
            return storeLongArrayField((long[]) value, singleAttribute, mmd);
        }
        else if (Short.TYPE.isAssignableFrom(type))
        {
            return storeShortArrayField((short[]) value, singleAttribute, mmd);
        }
        else if (String.class.isAssignableFrom(type))
        {
            return storeObjectArrayField((String[]) value, singleAttribute, mmd);
        }
        else if (Boolean.class.isAssignableFrom(type))
        {
            return storeBooleanObjectArrayField((Boolean[]) value, singleAttribute, mmd);
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            return storeByteObjectArrayField((Byte[]) value, singleAttribute, mmd);
        }
        else if (Character.class.isAssignableFrom(type))
        {
            return storeCharacterObjectArrayField((Character[]) value, singleAttribute, mmd);
        }
        else if (Double.class.isAssignableFrom(type))
        {
            return storeDoubleObjectArrayField((Double[]) value, singleAttribute, mmd);
        }
        else if (Float.class.isAssignableFrom(type))
        {
            return storeFloatObjectArrayField((Float[]) value, singleAttribute, mmd);
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            return storeIntegerObjectArrayField((Integer[]) value, singleAttribute, mmd);
        }
        else if (Long.class.isAssignableFrom(type))
        {
            return storeLongObjectArrayField((Long[]) value, singleAttribute, mmd);
        }
        else if (Short.class.isAssignableFrom(type))
        {
            return storeShortObjectArrayField((Short[]) value, singleAttribute, mmd);
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
                converter = typeMgr.getTypeConverterForType(type, String.class);
            }
            if (converter != null)
            {
                Object[] values = (Object[]) value;
                String[] stringValues = new String[values.length];
                for (int i = 0; i < values.length; i++)
                {
                    stringValues[i] = (String) converter.toDatastoreType(values[i]);
                }
                return storeObjectArrayField(stringValues, singleAttribute, mmd);
            }
            else if (type.isEnum())
            {
                return storeObjectArrayField((Enum[]) value, singleAttribute, mmd);
            }
        }

        // TODO Localise this
        throw new NucleusException("Field " + mmd.getFullFieldName() + " cannot be persisted because type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    protected static Attribute storeBooleanArrayField(boolean[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromBooleanArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        List<String> strings = new ArrayList<String>();
        for (boolean v : values)
        {
            // must be upper case (TRUE/FALSE)
            strings.add(("" + v).toUpperCase());
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeBooleanObjectArrayField(Boolean[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromBooleanObjectArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        List<String> strings = new ArrayList<String>();
        for (boolean v : values)
        {
            // must be upper case (TRUE/FALSE)
            strings.add(("" + v).toUpperCase());
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeByteArrayField(byte[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            String name = LDAPUtils.getAttributeNameForField(mmd);
            return new BasicAttribute(name, values);
        }

        List<String> strings = new ArrayList<String>();
        for (byte v : values)
        {
            strings.add("" + v);
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeByteObjectArrayField(Byte[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromByteObjectArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        return storeObjectArrayField(values, false, mmd);
    }

    protected static Attribute storeCharArrayField(char[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromCharArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        List<String> strings = new ArrayList<String>();
        for (char v : values)
        {
            strings.add("" + v);
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeCharacterObjectArrayField(Character[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromCharObjectArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        return storeObjectArrayField(values, false, mmd);
    }

    protected static Attribute storeDoubleArrayField(double[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromDoubleArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        List<String> strings = new ArrayList<String>();
        for (double v : values)
        {
            strings.add("" + v);
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeDoubleObjectArrayField(Double[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromDoubleObjectArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        return storeObjectArrayField(values, false, mmd);
    }

    protected static Attribute storeFloatArrayField(float[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromFloatArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        List<String> strings = new ArrayList<String>();
        for (float v : values)
        {
            strings.add("" + v);
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeFloatObjectArrayField(Float[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromFloatObjectArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        return storeObjectArrayField(values, false, mmd);
    }

    protected static Attribute storeIntArrayField(int[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromIntArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        List<String> strings = new ArrayList<String>();
        for (int v : values)
        {
            strings.add("" + v);
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeIntegerObjectArrayField(Integer[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromIntObjectArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        return storeObjectArrayField(values, false, mmd);
    }

    protected static Attribute storeLongArrayField(long[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromLongArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        List<String> strings = new ArrayList<String>();
        for (long v : values)
        {
            strings.add("" + v);
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeLongObjectArrayField(Long[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromLongObjectArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        return storeObjectArrayField(values, false, mmd);
    }

    protected static Attribute storeShortArrayField(short[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromShortArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        List<String> strings = new ArrayList<String>();
        for (short v : values)
        {
            strings.add("" + v);
        }
        return storeStringList(strings, mmd);
    }

    protected static Attribute storeShortObjectArrayField(Short[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        if (singleAttribute)
        {
            byte[] bytes = TypeConversionHelper.getByteArrayFromShortObjectArray(values);
            return storeByteArrayField(bytes, true, mmd);
        }

        return storeObjectArrayField(values, false, mmd);
    }

    /**
     * Stores the string representation of each array element as multi-valued attribute.
     * @param values the values
     */
    protected static Attribute storeObjectArrayField(Object[] values, boolean singleAttribute, AbstractMemberMetaData mmd)
    {
        List<String> strings = new ArrayList<String>();
        for (Object v : values)
        {
            strings.add("" + v);
        }
        return storeStringList(strings, mmd);
    }

    /**
     * Stores the each list element as multi-valued attribute.
     * @param values the values
     */
    protected static Attribute storeStringList(List<String> values, AbstractMemberMetaData mmd)
    {
        String name = LDAPUtils.getAttributeNameForField(mmd);
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
        return attribute;
    }
}
