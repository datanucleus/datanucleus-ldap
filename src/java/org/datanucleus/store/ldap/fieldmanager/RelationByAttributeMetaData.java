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

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.JoinMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.ldap.LDAPUtils;

/**
 * Helper class for relationship mapping by attribute.
 */
public class RelationByAttributeMetaData
{

    private AbstractMemberMetaData mmd;

    private AbstractMemberMetaData otherMmd;

    private JoinMetaData joinMetaData;

    private String emptyValue;

    private boolean hasColumn;

    public RelationByAttributeMetaData(AbstractMemberMetaData mmd, MetaDataManager mmgr)
    {
        this.mmd = mmd;

        AbstractClassMetaData otherClassMetaData = LDAPUtils.getEffectiveClassMetaData(mmd, mmgr);

        hasColumn = mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0;

        if (mmd.getMappedBy() != null)
        {
            otherMmd = otherClassMetaData.getMetaDataForMember(mmd.getMappedBy());
        }
        else if (mmd.getElementMetaData() != null && !hasColumn)
        {
            // always null
            otherMmd = null;
        }
        else if (mmd.getJoinMetaData() != null)
        {
            // null at unidir
            String otherAttributeName = mmd.getJoinMetaData().getColumnName();
            otherMmd = LDAPUtils.getMemberMetadataForAttributeName(otherClassMetaData, otherAttributeName);
        }

        if (mmd.getMappedBy() != null)
        {
            joinMetaData = otherMmd.getJoinMetaData();
        }
        else
        {
            joinMetaData = mmd.getJoinMetaData();
        }

        if (mmd.getMappedBy() != null)
        {
            emptyValue = LDAPUtils.getEmptyValue(otherMmd);
        }
        else
        {
            emptyValue = LDAPUtils.getEmptyValue(mmd);
        }
    }

    /**
     * Returns the owner member meta data
     * @return the owner member meta data, may be null for an unidirectional relationship
     */
    public AbstractMemberMetaData getOwnerMMD()
    {
        if (mmd.getMappedBy() != null)
        {
            // mapped-by is always non-owner
            return otherMmd;
        }

        if (mmd.getElementMetaData() != null && !hasColumn)
        {
            // element is always non-owner
            return null;
        }

        return mmd;
    }

    public String getJoinAttributeName()
    {
        return joinMetaData.getColumnName();
    }

    /**
     * Gets the empty value
     * @return the empty value or null if none defined
     */
    public String getEmptyValue()
    {
        return emptyValue;
    }

    /**
     * Returns the attribute name of the relationship owner
     */
    public String getOwnerAttributeName()
    {
        if (mmd.getMappedBy() != null)
        {
            // mapped-by is always non-owner
            return LDAPUtils.getAttributeNameForField(otherMmd);
        }

        if (mmd.getElementMetaData() != null && !hasColumn)
        {
            // element is always non-owner
            return mmd.getElementMetaData().getColumnName();
        }

        return LDAPUtils.getAttributeNameForField(mmd);
    }

    /**
     * Returns the non-owner member meta data
     * @return the non-owner member meta data, may be null for an unidirectional relationship
     */
    public AbstractMemberMetaData getNonOwnerMMD()
    {
        if (mmd.getMappedBy() != null)
        {
            // mapped-by is always non-owner
            return mmd;
        }

        if (mmd.getElementMetaData() != null && !hasColumn)
        {
            // element is always non-owner
            return mmd;
        }

        return otherMmd;
    }

    public static boolean isRelationByAttribute(AbstractMemberMetaData mmd, MetaDataManager mmgr)
    {
        return new RelationByAttributeMetaData(mmd, mmgr).joinMetaData != null;
    }
}
