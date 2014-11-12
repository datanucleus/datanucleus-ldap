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
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.ldap.LDAPStoreManager;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.ldap.LDAPUtils.LocationInfo;

/**
 * Helper class for relationship mapping by DN.
 */
public class RelationByDnMetaData
{
    private AbstractMemberMetaData mmd;

    private AbstractMemberMetaData otherMmd;

    private String emptyValue;

    private boolean hasColumn;

    public RelationByDnMetaData(AbstractMemberMetaData mmd, MetaDataManager mmgr)
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
        // else ???

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

    /**
     * Gets the empty value
     * @return the empty value or null if none defined
     */
    public String getEmptyValue()
    {
        return emptyValue;
    }

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

    public static boolean isRelationByDn(AbstractMemberMetaData mmd, MetaDataManager mmgr)
    {
        if (mmd.hasExtension(LDAPStoreManager.MAPPING_STRATEGY_EXTENSON))
        {
            // User has specified the mapping-strategy explicitly via extension
            String mappingStrategy = mmd.getValueForExtension(LDAPStoreManager.MAPPING_STRATEGY_EXTENSON);
            if (mappingStrategy != null)
            {
                if (mappingStrategy.equalsIgnoreCase("attribute"))
                {
                    return false;
                }
                else if (mappingStrategy.equalsIgnoreCase("dn"))
                {
                    return true;
                }
            }
        }

        // no join
        // no {} in dn extension or table
        RelationByDnMetaData md = new RelationByDnMetaData(mmd, mmgr);
        if (md.mmd != null)
        {
            if (md.mmd.getJoinMetaData() != null)
            {
                return false;
            }
            AbstractClassMetaData effectiveClassMetaData = LDAPUtils.getEffectiveClassMetaData(md.mmd, mmgr);
            if (effectiveClassMetaData == null || effectiveClassMetaData.isEmbeddedOnly())
            {
                return false;
            }
            LocationInfo locationInfo = LDAPUtils.getLocationInfo(effectiveClassMetaData);
            if (locationInfo.parentFieldName != null)
            {
                return false;
            }
        }
        if (md.otherMmd != null)
        {
            if (md.otherMmd.getJoinMetaData() != null)
            {
                return false;
            }
            AbstractClassMetaData effectiveClassMetaData = LDAPUtils.getEffectiveClassMetaData(md.otherMmd, mmgr);
            if (effectiveClassMetaData == null || effectiveClassMetaData.isEmbeddedOnly())
            {
                return false;
            }
            LocationInfo locationInfo = LDAPUtils.getLocationInfo(effectiveClassMetaData);
            if (locationInfo.parentFieldName != null)
            {
                return false;
            }
        }
        return true;
    }
}
