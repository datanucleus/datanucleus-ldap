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

import java.util.List;

import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.ldap.LDAPUtils;

/**
 * Basic class for all mapping strategies. A mapping strategy is used to map values to LDAP attributes and entries.
 * TODO Remove StateManager from this since it is currently creating one of these per field per object!!
 */
public abstract class AbstractMappingStrategy
{
    protected ExecutionContext ec;

    /** StateManager. */
    protected DNStateManager sm;

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
     * @param sm StateManager
     * @param mmd Metadata for the member
     * @param attributes the attributes
     */
    protected AbstractMappingStrategy(DNStateManager sm, AbstractMemberMetaData mmd, Attributes attributes)
    {
        this.ec = sm.getExecutionContext();
        this.sm = sm;
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
}