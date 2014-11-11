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
 * FieldManager for retrieving field values from LDAP results.
 */
public class FetchFieldManager extends AbstractFieldManager
{
    ObjectProvider op;
    StoreManager storeMgr;
    Attributes result;

    // TODO Provide constructor that takes in ExecutionContext and AbstractClassMetaData so we can remove 
    // use of deprecated EC.findObjectUsingAID. This would mean that all XXXMappingStrategy take in ExecutionContext
    public FetchFieldManager(StoreManager storeMgr, ObjectProvider op, Attributes result)
    {
        this.op = op;
        this.storeMgr = storeMgr;
        this.result = result;
    }

    public String fetchStringField(int fieldNumber)
    {
        return (String) fetchObjectField(fieldNumber);
    }

    public Object fetchObjectField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        AbstractMappingStrategy ms = AbstractMappingStrategy.findMappingStrategy(storeMgr, op, mmd, result);
        if (ms != null)
        {
            return ms.fetch();
        }

        // TODO Localise this
        throw new NucleusException("Cant obtain value for field " + mmd.getFullFieldName() + " since type=" + mmd.getTypeName() + " is not supported for this datastore");
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        return (Boolean) fetchObjectField(fieldNumber);
    }

    public byte fetchByteField(int fieldNumber)
    {
        return (Byte) fetchObjectField(fieldNumber);
    }

    public char fetchCharField(int fieldNumber)
    {
        return (Character) fetchObjectField(fieldNumber);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        return (Double) fetchObjectField(fieldNumber);
    }

    public float fetchFloatField(int fieldNumber)
    {
        return (Float) fetchObjectField(fieldNumber);
    }

    public int fetchIntField(int fieldNumber)
    {
        return (Integer) fetchObjectField(fieldNumber);
    }

    public long fetchLongField(int fieldNumber)
    {
        return (Long) fetchObjectField(fieldNumber);
    }

    public short fetchShortField(int fieldNumber)
    {
        return (Short) fetchObjectField(fieldNumber);
    }
}