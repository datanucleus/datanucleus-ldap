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

import javax.naming.directory.Attributes;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

/**
 * FieldManager for inserting data into LDAP.
 */
public class StoreFieldManager extends AbstractFieldManager
{
    ObjectProvider op;
    StoreManager storeMgr;
    Attributes attributes;
    boolean insert;

    public StoreFieldManager(StoreManager storeMgr, ObjectProvider sm, Attributes attrs, boolean insert)
    {
        this.op = sm;
        this.storeMgr = storeMgr;
        this.attributes = attrs;
        this.insert = insert;
    }

    public void storeStringField(int fieldNumber, String value)
    {
        storeObjectField(fieldNumber, value);
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        AbstractMappingStrategy ms = AbstractMappingStrategy.findMappingStrategy(storeMgr, op, mmd, attributes);
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
        storeObjectField(fieldNumber, value);
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        storeObjectField(fieldNumber, value);
    }

    public void storeCharField(int fieldNumber, char value)
    {
        storeObjectField(fieldNumber, value);
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        storeObjectField(fieldNumber, value);
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        storeObjectField(fieldNumber, value);
    }

    public void storeIntField(int fieldNumber, int value)
    {
        storeObjectField(fieldNumber, value);
    }

    public void storeLongField(int fieldNumber, long value)
    {
        storeObjectField(fieldNumber, value);
    }

    public void storeShortField(int fieldNumber, short value)
    {
        storeObjectField(fieldNumber, value);
    }
}