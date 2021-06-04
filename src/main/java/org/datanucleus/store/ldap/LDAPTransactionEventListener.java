/**********************************************************************
Copyright (c) 2008 Stefan Seelmann and others. All rights reserved.
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
package org.datanucleus.store.ldap;

import java.util.HashSet;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.transaction.TransactionEventListener;

/**
 * Transaction Event Listener for the LDAP store.
 */
public class LDAPTransactionEventListener implements TransactionEventListener
{
    Set<Object> objectsToPersist = new HashSet<Object>();

    Set<Object> objectsToDelete = new HashSet<Object>();

    Set<Object> objectsNotToDelete = new HashSet<Object>();

    ExecutionContext ec;

    public LDAPTransactionEventListener(ExecutionContext ec)
    {
        this.ec = ec;
    }

    public void addObjectToDelete(Object pc)
    {
        if (!objectsNotToDelete.contains(pc))
        {
            objectsToDelete.add(pc);
        }
    }

    public void removeObjectToDelete(Object pc)
    {
        objectsToDelete.remove(pc);
        objectsNotToDelete.add(pc);
    }

    public void addObjectToPersist(Object pc)
    {
        objectsToPersist.add(pc);
    }

    public void transactionStarted() {}
    public void transactionEnded() {}
    public void transactionPreFlush() {}
    public void transactionFlushed() {}

    public void transactionPreCommit()
    {
        // insert
        while (!objectsToPersist.isEmpty())
        {
            HashSet<Object> insert = new HashSet<Object>(objectsToPersist);
            for (Object pc : insert)
            {
                ec.findObjectProvider(pc, true);
            }
            objectsToPersist.removeAll(insert);
        }

        // delete
        ec.deleteObjects(objectsToDelete.toArray());
    }

    public void transactionCommitted() {}
    public void transactionPreRollBack() {}
    public void transactionRolledBack() {}
    public void transactionSetSavepoint(String name) {}
    public void transactionReleaseSavepoint(String name) {}
    public void transactionRollbackToSavepoint(String name) {}
}