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
package org.datanucleus.store.ldap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.ContextNotEmptyException;
import javax.naming.InvalidNameException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.ldap.fieldmanager.FetchFieldManager;
import org.datanucleus.store.ldap.fieldmanager.StoreFieldManager;
import org.datanucleus.store.query.compiler.QueryCompilation;
import org.datanucleus.transaction.Transaction;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Convenience methods for LDAP datastores.
 */
public class LDAPUtils
{
    public static final String[] NO_ATTRIBUTES = new String[0];

    /**
     * Gets the effective class meta data for the given field meta data. This is either the class meta data of field
     * itself or, if the field is a collection type, the class meta data of the collection's elements.
     * @param mmd the field meta data
     * @param mmgr Metadata manager
     * @return the effective class meta data, null if none found
     */
    public static AbstractClassMetaData getEffectiveClassMetaData(AbstractMemberMetaData mmd, MetaDataManager mmgr)
    {
        ClassLoaderResolver clr = mmgr.getNucleusContext().getClassLoaderResolver(null);

        String[] fieldTypes = mmd.getFieldTypes();
        if(fieldTypes != null && fieldTypes.length > 0)
        {
            AbstractClassMetaData fieldTypeCmd = mmgr.getMetaDataForClass(fieldTypes[0], clr);
            if (fieldTypeCmd != null)
            {
                return fieldTypeCmd;
            }
        }

        AbstractClassMetaData fieldTypeCmd = mmgr.getMetaDataForClass(mmd.getType(), clr);
        if (fieldTypeCmd != null)
        {
            return fieldTypeCmd;
        }

        AbstractClassMetaData collectionFieldTypeCmd =
            (mmd.getCollection() != null) ? mmd.getCollection().getElementClassMetaData(clr) : null;
        if (collectionFieldTypeCmd != null)
        {
            return collectionFieldTypeCmd;
        }

        return null;
    }

    /*
     * Helpers for hierarchical mapping
     */

    /**
     * Gets the member metadata from the given class metadata which attribute name matches the given LDAP attribute
     * name.
     * @param cmd the class metadata
     * @param attributeName the LDAP attribute name
     * @return the member metadata which attribute name matches, null if not found
     */
    public static AbstractMemberMetaData getMemberMetadataForAttributeName(AbstractClassMetaData cmd, String attributeName)
    {
        if (cmd != null && attributeName != null)
        {
            List<AbstractMemberMetaData> allMemberMetaData = getAllMemberMetaData(cmd);
            for (AbstractMemberMetaData mmd : allMemberMetaData)
            {
                String attributeNameForField = getAttributeNameForField(mmd);
                if (attributeName.equals(attributeNameForField))
                {
                    return mmd;
                }
            }
        }
        return null;
    }

    public static List<AbstractMemberMetaData> getAllMemberMetaData(AbstractClassMetaData cmd)
    {
        return getMemberMetaData(cmd.getAllMemberPositions(), cmd);
    }

    public static List<AbstractMemberMetaData> getMemberMetaData(int[] fieldNumbers, AbstractClassMetaData cmd)
    {
        List<AbstractMemberMetaData> mmds = new ArrayList<AbstractMemberMetaData>();
        if (fieldNumbers != null && fieldNumbers.length > 0)
        {
            for (int fieldNumber : fieldNumbers)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
                mmds.add(mmd);
            }
        }
        return mmds;
    }

    public static boolean isEmbeddedField(AbstractMemberMetaData mmd)
    {
        return mmd.getEmbeddedMetaData() != null || (mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null);
    }
    
    /**
     * Convenience method to check if the given object is mapped hierarchical.
     * @param sm ObjectProvider
     * @return true if is hierarchical mapped
     */
    public static boolean isHierarchicalMappedAtChild(ObjectProvider sm)
    {
        LocationInfo locationInfo = getLocationInfo(sm.getClassMetaData());
        return locationInfo.parentFieldName != null;
    }

    /**
     * Gets the distinguished name of the parent object
     * @param dn The distinguished name
     * @param suffix the suffix defined in "dn" extension
     * @return the distinguished name of the parent object
     */
    public static LdapName getParentDistingueshedName(LdapName dn, LdapName suffix)
    {
        LdapName parent = (LdapName) dn.getPrefix(dn.size() - suffix.size() - 1);
        return parent;
    }

    /**
     * Gets the relative distinguished name for object.
     * @param sm StateManager
     * @return the RDN for object
     */
    private static Rdn getRdnForObject(StoreManager storeMgr, ObjectProvider sm) throws InvalidNameException
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        // TODO Cater for composite PK
        int fieldNumber = cmd.getPKMemberPositions()[0];
//        Object value = op.provideField(fieldNumber);
//        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        Attributes rdnAttributes = new BasicAttributes();

        StoreFieldManager storeFM = new StoreFieldManager(storeMgr, sm, rdnAttributes, true);
        sm.provideFields(new int[]{fieldNumber}, storeFM);
//        AbstractMappingStrategy ms = MappingStrategyHelper.findMappingStrategy(storeMgr, op, mmd, rdnAttributes);
//        ms.insert(value);

        return new Rdn(rdnAttributes);
    }

    /**
     * Convenience method to return the distinguished name for the object being managed. Uses the extension "dn" if
     * specified, else the "table" if specified as parent container. Uses the PK name and value as RDN.
     * @param storeMgr Store Manager
     * @param sm StateManager
     * @return Distinguished name
     */
    public static LdapName getDistinguishedNameForObject(StoreManager storeMgr, ObjectProvider sm)
    {
        return getDistinguishedNameForObject(storeMgr, sm, false);
    }

    /**
     * Convenience method to return the distinguished name for the object being managed. Uses the extension "dn" if
     * specified, else the "table" if specified as parent container. Uses the PK name and value as RDN.
     * @param storeMgr Store Manager
     * @param sm StateManager
     * @param forceFetchHierarchicalMappedDn true to fetch the name from directory server
     * @return Distinguished name
     */
    public static LdapName getDistinguishedNameForObject(StoreManager storeMgr, ObjectProvider sm, boolean forceFetchHierarchicalMappedDn)
    {
        return getDistinguishedNameForObject(storeMgr, sm, null, forceFetchHierarchicalMappedDn);
    }

    private static LdapName getDistinguishedNameForObject(StoreManager storeMgr, ObjectProvider sm, Set<ObjectProvider> handledOPs,
            boolean forceFetchHierarchicalMappedDn)
    {
        if (handledOPs == null)
        {
            handledOPs = new HashSet<ObjectProvider>();
        }
        if (handledOPs.contains(sm))
        {
            throw new NucleusDataStoreException("Recursive loop detected while creating distinguished name for " + sm);
        }
        handledOPs.add(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        LocationInfo locationInfo = getLocationInfo(cmd);

        LdapName dn;
        ExecutionContext ec = sm.getExecutionContext();
        try
        {
            SearchControls searchControls = getSearchControls(cmd);
            ObjectProvider[] embOwnerSMs = ec.getOwnersForEmbeddedObjectProvider(sm);
            if (embOwnerSMs != null && embOwnerSMs.length > 0)
            {
                ObjectProvider embOwnerSM = embOwnerSMs[0];
                dn = getDistinguishedNameForObject(storeMgr, embOwnerSM, handledOPs, forceFetchHierarchicalMappedDn);
                dn.add(getRdnForObject(storeMgr, sm));
            }
            else if (searchControls.getSearchScope() == SearchControls.OBJECT_SCOPE)
            {
                dn = locationInfo.dn;
            }
            else if (isHierarchicalMappedAtChild(sm))
            {
                Rdn rdn = getRdnForObject(storeMgr, sm);
                LdapName parentDn;
                AbstractMemberMetaData parentFieldMmd = cmd.getMetaDataForMember(locationInfo.parentFieldName);
                if (forceFetchHierarchicalMappedDn)
                {
                    // search for DN, use parent-DN as base
                    AbstractClassMetaData parentFieldTypeCmd = ec.getMetaDataManager().getMetaDataForClass(parentFieldMmd.getType(), ec.getClassLoaderResolver());
                    LdapName base = getSearchBase(parentFieldTypeCmd, ec.getMetaDataManager());
                    String ocFilter = getSearchFilter(cmd);
                    String rdnFilter = "(" + rdn.getType() + "=" + rdn.getValue() + ")";
                    String filter = ocFilter != null ? "(&" + ocFilter + rdnFilter + ")" : rdnFilter;
                    ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
                    try
                    {
                        DirContext ctx = (DirContext) mconn.getConnection();
                        if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("LDAP.JNDI.search", base, filter, searchControls.getSearchScope()));
                        }
                        NamingEnumeration<SearchResult> enumeration = ctx.search(base, filter, searchControls);
                        if (enumeration.hasMoreElements())
                        {
                            SearchResult sr = enumeration.nextElement();
                            String srName = sr.getNameInNamespace();
                            LdapName srDn = new LdapName(srName);
                            parentDn = getParentDistingueshedName(srDn, locationInfo.suffix);
                            enumeration.close();
                        }
                        else
                        {
                            if (locationInfo.dn != null)
                            {
                                parentDn = locationInfo.dn;
                            }
                            else
                            {
                                throw new NucleusObjectNotFoundException("No distinguished name found for object " + sm.toString());
                            }
                        }
                    }
                    catch (NamingException e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }
                    finally
                    {
                        mconn.release();
                    }
                }
                else
                {
                    Object parentFieldValue = sm.provideField(parentFieldMmd.getAbsoluteFieldNumber());
                    if (parentFieldValue != null)
                    {
                        // compose DN using parent DN
                        boolean detached = ec.getApiAdapter().isDetached(parentFieldValue);
                        ObjectProvider parentSm = ec.findObjectProvider(parentFieldValue, detached);
                        if (parentSm == null)
                        {
                            throw new NucleusObjectNotFoundException("No state manager found for object " + parentFieldValue);
                        }
                        parentDn = getDistinguishedNameForObject(storeMgr, parentSm, handledOPs, forceFetchHierarchicalMappedDn);
                    }
                    else if (locationInfo.dn != null)
                    {
                        parentDn = locationInfo.dn;
                    }
                    else
                    {
                        // no way to get the DN from the object, fetch it from directory
                        handledOPs.remove(sm);
                        LdapName smDn = getDistinguishedNameForObject(storeMgr, sm, handledOPs, true);
                        parentDn = getParentDistingueshedName(smDn, locationInfo.suffix);
                    }
                }

                dn = composeDistinguishedName(parentDn, rdn, locationInfo.suffix);
            }
            else
            {
                dn = new LdapName(locationInfo.dn.toString());
                dn.add(getRdnForObject(storeMgr, sm));
            }
        }
        catch (InvalidNameException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }

        return dn;
    }

    public static LdapName getSearchBase(AbstractClassMetaData cmd, MetaDataManager mmgr)
    {
        LdapName dn;
        try
        {
            LocationInfo locationInfo = getLocationInfo(cmd);
            if (locationInfo.parentFieldName != null && locationInfo.dn == null)
            {
                AbstractMemberMetaData parentMmd = cmd.getMetaDataForMember(locationInfo.parentFieldName);
                ClassLoaderResolver clr = mmgr.getNucleusContext().getClassLoaderResolver(null);
                AbstractClassMetaData parentFieldCmd = mmgr.getMetaDataForClass(parentMmd.getType(), clr);
                LdapName parentDn = getSearchBase(parentFieldCmd, mmgr);

                dn = composeDistinguishedName(parentDn, null, locationInfo.suffix);
            }
            else
            {
                dn = locationInfo.dn;
            }
        }
        catch (InvalidNameException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        return dn;
    }

    /**
     * Gets the search controls. Contains scope SUBTREE_SCOPE in case hierarchical mapping is used, scope ONELEVEL_SCOPE
     * otherwise.
     * @param cmd the class meta data
     * @return the search controls
     */
    public static SearchControls getSearchControls(AbstractClassMetaData cmd)
    {
        SearchControls searchControls = new SearchControls();

        LocationInfo locationInfo = getLocationInfo(cmd);
        if (locationInfo.parentFieldName != null)
        {
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        }

        // overwrite scope if present in an LDAP URL
        if (locationInfo.scope != -1)
        {
            searchControls.setSearchScope(locationInfo.scope);
        }

        return searchControls;
    }

    /**
     * Convenience method to get the LDAP filter for the specified class.
     * @param cmd the class meta data
     * @return the object classes filter the specified class, null if no object class
     */
    public static String getSearchFilter(AbstractClassMetaData cmd)
    {
        // check for a filter in the LDAP URL
        LocationInfo locationInfo = getLocationInfo(cmd);
        String urlFilter = locationInfo.filter;

        Set<String> objectClasses = getObjectClassesForClass(cmd);
        if (objectClasses.isEmpty())
        {
            return null;
        }

        StringBuilder filter = new StringBuilder();
        if (objectClasses.size() > 1 || urlFilter != null)
        {
            filter.append("(&");
            for (String oc : objectClasses)
            {
                filter.append("(objectClass=");
                filter.append(oc);
                filter.append(")");
            }
            if (urlFilter != null)
            {
                filter.append(urlFilter);
            }
            filter.append(")");
        }
        else
        {
            filter.append("(objectClass=");
            filter.append(objectClasses.iterator().next());
            filter.append(")");
        }
        return filter.toString();
    }

    /**
     * Convenience method to return the distinguished name pattern for the metadata being managed. Uses the extension
     * "dn" if specified, else the "table" if specified.
     * @param cmd the class meta data
     * @return the DN as String
     */

    public static LocationInfo getLocationInfo(AbstractClassMetaData cmd)
    {
        String raw = null;
        if (cmd != null && cmd.hasExtension("dn"))
        {
            raw = cmd.getValueForExtension("dn");
        }
        else if (cmd != null && cmd.getTable() != null)
        {
            raw = cmd.getTable();
        }

        LocationInfo li = new LocationInfo();

        // parse the string
        // TODO: use org.apache.directory.shared.ldap.util.LdapURL class?
        if (raw != null)
        {
            String dnOrParentField = null;
            if (raw.startsWith("ldap:///"))
            {
                // format: ldap:///dn?attributes?scope?filter?extensions
                raw = raw.substring("ldap:///".length());
                String[] split = raw.split("\\?", 5);
                // dn
                if (split.length > 0)
                {
                    dnOrParentField = split[0];
                }
                // attributes
                if (split.length > 1)
                {
                    // ignore
                }
                // scope
                if (split.length > 2)
                {
                    String scope = split[2];
                    if (scope.length() > 0)
                    {
                        if ("base".equals(scope))
                        {
                            li.scope = SearchControls.OBJECT_SCOPE;
                        }
                        else if ("one".equals(scope))
                        {
                            li.scope = SearchControls.ONELEVEL_SCOPE;
                        }
                        else if ("sub".equals(scope))
                        {
                            li.scope = SearchControls.SUBTREE_SCOPE;
                        }
                        else
                        {
                            throw new NucleusDataStoreException("Invalid scope in LDAP URL: " + scope);
                        }
                    }
                }
                // filter
                if (split.length > 3)
                {
                    String filter = split[3];
                    if (filter.length() > 0)
                    {
                        li.filter = filter;
                    }
                }
            }
            else
            {
                dnOrParentField = raw;
            }

            if (dnOrParentField != null)
            {
                int left = dnOrParentField.indexOf('{');
                int right = dnOrParentField.indexOf('}');
                if (left > -1 && right > left)
                {
                    li.parentFieldName = dnOrParentField.substring(left + 1, right);

                    try
                    {
                        String suffix = dnOrParentField.substring(0, left);
                        LdapName suffixDn = new LdapName(suffix);
                        if (suffixDn.size() > 0 && suffixDn.getRdn(0).size() == 0)
                        {
                            suffixDn.remove(0);
                        }
                        li.suffix = suffixDn;
                    }
                    catch (InvalidNameException e)
                    {
                        throw new NucleusDataStoreException("Invalid LDAP DN: " + dnOrParentField);
                    }

                    if (dnOrParentField.length() > right + 2 && dnOrParentField.charAt(right + 1) == '|')
                    {
                        try
                        {
                            li.dn = new LdapName(dnOrParentField.substring(right + 2, dnOrParentField.length()));
                        }
                        catch (InvalidNameException e)
                        {
                            throw new NucleusDataStoreException("Invalid LDAP DN: " + dnOrParentField);
                        }
                    }
                }
                else
                {
                    try
                    {
                        li.dn = new LdapName(dnOrParentField);
                    }
                    catch (InvalidNameException e)
                    {
                        throw new NucleusDataStoreException("Invalid LDAP DN: " + dnOrParentField);
                    }
                }
            }

        }

        return li;
    }

    /**
     * Information where an object is located in the LDAP tree.
     */
    public static class LocationInfo
    {
        /** The LDAP distinguished name, null if none */
        public LdapName dn = null;

        /** The parent field name, null if none */
        public String parentFieldName = null;

        /** The suffix, only if parent field name is set, null if none */
        public LdapName suffix = null;

        /** The filter if the LDAP URL, null if none */
        public String filter = null;

        /** The search scope, -1 if none */
        public int scope = -1;
    }

    /**
     * Convenience method to return value defined in extension "empty-value" of the given field meta data, null if not
     * specified. Used as workaround for attributes member (object class groupOfNames) and uniqueMemeber (object
     * classgroupOfUniqueNames) which must contain at least one value
     * @param mmd the member meta data
     * @return the value
     */
    public static String getEmptyValue(AbstractMemberMetaData mmd)
    {
        return mmd.getValueForExtension("empty-value");
    }

    /*
     * Helper for MetaData <-> LDAP translation
     */

    /**
     * Gets the attribute value of an specific attribute name from the ObjectProvider.
     * @param storeMgr Store Manager
     * @param sm StateManager
     * @param attributeName the attribute name
     * @return the attribute value
     */
    public static Object getAttributeValue(StoreManager storeMgr, ObjectProvider sm, String attributeName)
    {
        try
        {
            // get field value of the PC
            AbstractMemberMetaData pcMmd = LDAPUtils.getMemberMetadataForAttributeName(sm.getClassMetaData(), attributeName);
            if (pcMmd == null)
            {
                throw new NucleusUserException("Tried to find LDAP attribute " + attributeName + " in class " + sm.getClassMetaData().getFullClassName() + " but not found. Metadata wrong?");
            }

            // get LDAP value
            Attributes pcAttributes = new BasicAttributes();
            StoreFieldManager storeFM = new StoreFieldManager(storeMgr, sm, pcAttributes, true);
            sm.provideFields(new int[]{pcMmd.getAbsoluteFieldNumber()}, storeFM);
            Attribute pcAttribute = pcAttributes.get(attributeName);
            return pcAttribute.get();
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    /**
     * Gets the attribute value of an specific attribute name from LDAP.
     * @param storeMgr Store Manager
     * @param sm StateManager
     * @param attributeName the attribute name
     * @return the attribute value
     */
    public static Collection<Object> getAttributeValuesFromLDAP(StoreManager storeMgr, ObjectProvider sm, String attributeName)
    {
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(sm.getExecutionContext());
        try
        {
            LdapName dn = getDistinguishedNameForObject(storeMgr, sm, true);
            DirContext ctx = (DirContext) mconn.getConnection();
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.getAttributes", dn, attributeName, ""));
            }
            Attributes attributes = ctx.getAttributes(dn, new String[]{attributeName});
            Attribute attribute = attributes.get(attributeName);
            Collection<Object> pcAttributeValues = new ArrayList<Object>();
            if (attribute != null)
            {
                NamingEnumeration<?> all = attribute.getAll();
                while (all.hasMoreElements())
                {
                    pcAttributeValues.add(all.nextElement());
                }
            }
            return pcAttributeValues;
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Accessor for the (LDAP) attribute name for the specified field. Uses the extension "dn" if specified
     * (deprecated), else the extension "attribute" if specified, else the "column" if specified, else uses the field
     * name.
     * @param mmd MetaData for the field
     * @return The attribute
     */
    public static String getAttributeNameForField(AbstractMemberMetaData mmd)
    {
        String name = mmd.getName();
        if (mmd.hasExtension("dn"))
        {
            name = mmd.getValueForExtension("dn");
        }
        else if (mmd.hasExtension("attribute"))
        {
            name = mmd.getValueForExtension("attribute");
        }
        else if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
        {
            name = mmd.getColumnMetaData()[0].getName();
        }
        return name;
    }

    /**
     * Accessor for the (LDAP) object classes for the specified class. Uses the extension "objectClass" if specified,
     * else the "schema" if specified. name.
     * @param cmd Metadata for the class
     * @return The object classes
     */
    public static Set<String> getObjectClassesForClass(AbstractClassMetaData cmd)
    {
        Set<String> ocs = new LinkedHashSet<String>();
        if (cmd.hasExtension("objectClass"))
        {
            ocs.addAll(Arrays.asList(cmd.getValuesForExtension("objectClass")));
        }
        else if (cmd.getSchema() != null)
        {
            ocs.addAll(Arrays.asList(MetaDataUtils.getInstance().getValuesForCommaSeparatedAttribute(cmd.getSchema())));
        }
        return ocs;
    }

    /**
     * Composes the distinguished name.
     * @param parentDn the parent dn
     * @param rdn the rdn
     * @param suffix the suffix
     * @return the ldap name
     * @throws InvalidNameException if error occurs
     */
    public static LdapName composeDistinguishedName(LdapName parentDn, Rdn rdn, LdapName suffix) throws InvalidNameException
    {
        LdapName dn = new LdapName(parentDn.getRdns());
        dn.addAll(suffix.getRdns());
        if (rdn != null)
        {
            dn.add(rdn);
        }
        return dn;
    }

    public static Object getObjectByDN(StoreManager storeMgr, ExecutionContext om, Class type, String dnAsString)
    {
        AbstractClassMetaData cmd = om.getMetaDataManager().getMetaDataForClass(type, om.getClassLoaderResolver());
        LdapName base = getSearchBase(cmd, om.getMetaDataManager());

        try
        {
            LdapName dn = new LdapName(dnAsString);
            Rdn rdn = dn.getRdn(dn.size() - 1);
            String filter = "(" + rdn.getType() + "=" + rdn.getValue() + ")";
            List<Object> objects = LDAPUtils.getObjectsOfCandidateType(storeMgr, om, cmd, base, filter, true, false);
            if (objects.size() == 1)
            {
                return objects.get(0);
            }
            else if (objects.size() == 0)
            {
                // TODO: localise
                throw new NucleusObjectNotFoundException("No object found with type " + type + " and filter " + filter);
            }
            else
            {
                // TODO: localise
                throw new NucleusDataStoreException("Unambiguous match with type " + type + " and filter " + filter);
            }
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    /**
     * Gets an object by type and attribute.
     * @param storeMgr Store Manager
     * @param ec object manager
     * @param type the object type
     * @param attributeName the attribute name
     * @param attributeValue the attribute value
     * @param mmgr the meta data manager
     * @return The object
     */
    public static Object getObjectByAttribute(StoreManager storeMgr, ExecutionContext ec, Class type, String attributeName, String attributeValue,
            MetaDataManager mmgr)
    {
        String attributeFilter = "(" + attributeName + "=" + attributeValue + ")";
        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(type, ec.getClassLoaderResolver());
        LdapName base = getSearchBase(cmd, mmgr);
        List<Object> objects = LDAPUtils.getObjectsOfCandidateType(storeMgr, ec, cmd, base, attributeFilter, true, false);
        if (objects.size() == 1)
        {
            return objects.get(0);
        }
        else if (objects.size() == 0)
        {
            // TODO: localise
            throw new NucleusObjectNotFoundException("No object found with type " + type + " and filter " + attributeFilter);
        }
        else
        {
            // TODO: localise
            throw new NucleusDataStoreException("Unambiguous match with type " + type + " and filter " + attributeFilter);
        }
    }

    public static void markForPersisting(Object pc, ExecutionContext ec)
    {
        LDAPTransactionEventListener listener = getTransactionEventListener(ec);
        listener.addObjectToPersist(pc);
    }

    public static void markForRename(StoreManager storeMgr, Object pc, ExecutionContext ec, LdapName oldDn, LdapName newDn)
    {
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.rename", oldDn, newDn));
            }
            ((DirContext) mconn.getConnection()).rename(oldDn, newDn);
        }
        catch (NamingException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public static void markForDeletion(Object pc, ExecutionContext ec)
    {
        LDAPTransactionEventListener listener = getTransactionEventListener(ec);
        listener.addObjectToDelete(pc);
    }

    public static void unmarkForDeletion(Object pc, ExecutionContext ec)
    {
        LDAPTransactionEventListener listener = getTransactionEventListener(ec);
        listener.removeObjectToDelete(pc);
    }

    private static LDAPTransactionEventListener getTransactionEventListener(ExecutionContext ec)
    {
        Transaction transaction = ec.getTransaction();
        Map<String, Object> txOptions = transaction.getOptions();
        LDAPTransactionEventListener listener = (txOptions != null ? (LDAPTransactionEventListener)txOptions.get("LDAPTransactionEventListener") : null);
        if (listener == null)
        {
            listener = new LDAPTransactionEventListener(ec);
            transaction.setOption("LDAPTransactionEventListener", listener);
            transaction.addTransactionEventListener(listener);
        }
        return listener;
    }

    /*
     * Search methods
     */

    /**
     * Convenience method to get all objects of the candidate type (and optional subclasses).
     * @param storeMgr Store Manager
     * @param ec The object manager
     * @param compilation The query
     * @param parameters The input parameters
     * @param candidateClass The class of the candidates
     * @param subclasses Include subclasses?
     * @param ignoreCache Whether to ignore the cache
     * @param inMemory Whether to filter in memory or to use native LDAP filters
     * @return List of objects of the candidate type (or subclass)
     */
    public static List<Object> getObjectsOfCandidateType(StoreManager storeMgr, ExecutionContext ec, QueryCompilation compilation, Map parameters,
            Class candidateClass, boolean subclasses, boolean ignoreCache, boolean inMemory)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
        List<Object> results = getObjectsOfCandidateType(storeMgr, ec, compilation, parameters, cmd, ignoreCache, inMemory);
        if (subclasses)
        {
            // Add on any subclass objects
            String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(candidateClass.getName(), true);
            if (subclassNames != null)
            {
                for (String subclassName : subclassNames)
                {
                    AbstractClassMetaData subCmd = ec.getMetaDataManager().getMetaDataForClass(subclassName, clr);
                    results.addAll(getObjectsOfCandidateType(storeMgr, ec, compilation, parameters, subCmd, ignoreCache, inMemory));
                }
            }
        }
        return results;
    }

    private static List<Object> getObjectsOfCandidateType(StoreManager storeMgr, ExecutionContext ec, QueryCompilation compilation, Map parameters,
            final AbstractClassMetaData acmd, boolean ignoreCache, boolean inMemory)
    {
        List<Object> results = new ArrayList<Object>();

        String classFilter = getSearchFilter(acmd);
        if (classFilter != null) // this is a sanity check, if the class does not have the objectClass
        {
            if (!inMemory)
            {
                try
                {
                    // this class cannot be linked at compile time since its dependencies are considered as optional
                    String className = "org.datanucleus.store.ldap.query.QueryToLDAPFilterMapper";
                    Class cls = ec.getClassLoaderResolver().classForName(className);
                    Method method = ClassUtils.getMethodForClass(cls, "compile", null);
                    Constructor constr = ClassUtils.getConstructorWithArguments(cls, new Class[]{QueryCompilation.class, Map.class, AbstractClassMetaData.class});
                    String filter = (String) method.invoke(constr.newInstance(new Object[]{compilation, parameters, acmd}), (Object[]) null);

                    results = getObjectsOfCandidateType(storeMgr, ec, acmd, null, filter, ignoreCache);
                }
                catch (Throwable e)
                {
                    NucleusLogger.QUERY.warn(Localiser.msg("LDAP.Query.NativeQueryFailed"));
                    // on error switch back to in-memory handling
                    inMemory = true;
                }
            }
            if (inMemory)
            {
                results = getObjectsOfCandidateType(storeMgr, ec, acmd, null, classFilter, ignoreCache);
            }
        }
        return results;
    }

    /**
     * @param storeMgr Store Manager
     * @param ec The object manager
     * @param candidateCmd The class meta data of the candidate
     * @param base the search base, or null to use the base of candidateCmd
     * @param additionalFilter the additional filter, or null to only use the filter of candidateCmd
     * @param subclasses true to include subclasses
     * @param ignoreCache whether to ignore the cache
     * @return List of objects
     */
    public static List<Object> getObjectsOfCandidateType(StoreManager storeMgr, ExecutionContext ec, final AbstractClassMetaData candidateCmd, LdapName base,
            String additionalFilter, boolean subclasses, boolean ignoreCache)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        List<Object> results = getObjectsOfCandidateType(storeMgr, ec, candidateCmd, base, additionalFilter, ignoreCache);
        if (subclasses)
        {
            // Add on any subclass objects
            String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(candidateCmd.getFullClassName(), true);
            if (subclassNames != null)
            {
                for (String subclassName : subclassNames)
                {
                    AbstractClassMetaData subCmd = ec.getMetaDataManager().getMetaDataForClass(subclassName, clr);
                    results.addAll(getObjectsOfCandidateType(storeMgr, ec, subCmd, base, additionalFilter, ignoreCache));
                }
            }
        }
        return results;
    }

    private static List<Object> getObjectsOfCandidateType(final StoreManager storeMgr, final ExecutionContext ec,
        final AbstractClassMetaData cmd, LdapName base, String additionalFilter, boolean ignoreCache)
    {
        List<Object> results = new ArrayList<Object>();
        final ClassLoaderResolver clr = ec.getClassLoaderResolver();
        Map<LdapName, Attributes> entries = getEntries(storeMgr, ec, cmd, base, additionalFilter, false, ignoreCache);
        for (final Attributes attrs : entries.values())
        {
            // TODO Drop usage of findObjectUsingAID (see NUCLDAP-48) and use IdentityUtils instead
            // The problem is that FetchFieldManager relies on having ObjectProvider available, which is wrong
            Object pc = findObjectUsingAID(ec, clr.classForName(cmd.getFullClassName()), new FieldValues()
            {
                // ObjectProvider calls the fetchFields method
                public void fetchFields(ObjectProvider sm)
                {
                    sm.replaceFields(cmd.getPKMemberPositions(), new FetchFieldManager(storeMgr, sm, attrs));
                    
                    List<AbstractMemberMetaData> allMemberMetaData = getAllMemberMetaData(cmd);
                    List<AbstractMemberMetaData> basicMemberMetaData = new ArrayList<AbstractMemberMetaData>();
                    for (AbstractMemberMetaData mmd : allMemberMetaData)
                    {
                        mmd.getAbsoluteFieldNumber();
                        if (mmd.getRelationType(clr) == RelationType.NONE && !mmd.isPersistentInterface(clr) && 
                            !Collection.class.isAssignableFrom(mmd.getType()) && !Map.class.isAssignableFrom(mmd.getType()) && !mmd.getType().isArray())
                        {
                            basicMemberMetaData.add(mmd);
                        }
                    }
                    int[] basicMemberPosition = new int[basicMemberMetaData.size()];
                    for (int i = 0; i < basicMemberMetaData.size(); i++)
                    {
                        basicMemberPosition[i] = basicMemberMetaData.get(i).getAbsoluteFieldNumber();
                    }
                    sm.replaceFields(basicMemberPosition, new FetchFieldManager(storeMgr, sm, attrs));
                }

                public void fetchNonLoadedFields(ObjectProvider sm)
                {
                    sm.replaceNonLoadedFields(cmd.getAllMemberPositions(), new FetchFieldManager(storeMgr, sm, attrs));
                }

                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, ignoreCache, true);

            results.add(pc);
        }

        return results;
    }

    /**
     * Accessor for the ObjectProvider of an object given the object AID.
     * Note that this is moved from ExecutionContextImpl since only LDAP uses it now.
     * @param ec ExecutionContext
     * @param pcCls Type of the PC object
     * @param fv The field values to be loaded
     * @param ignoreCache true if it must ignore the cache
     * @param checkInheritance Whether look to the database to determine which
     * class this object is. This parameter is a hint. Set false, if it's
     * already determined the correct pcClass for this pc "object" in a certain
     * level in the hierarchy. Set to true and it will look to the database.
     * @return Object
     */
    protected static Object findObjectUsingAID(ExecutionContext ec, Class pcCls, final FieldValues fv, boolean ignoreCache, boolean checkInheritance)
    {
        // Create ObjectProvider to generate an identity NOTE THIS IS VERY INEFFICIENT
        ObjectProvider sm = ec.getNucleusContext().getObjectProviderFactory().newForHollowPopulatedAppId(ec, pcCls, fv);
        if (!ignoreCache)
        {
            // Check the cache
            Object oid = sm.getInternalObjectId();
            Object pc = ec.getObjectFromCache(oid);
            if (pc != null)
            {
                sm = ec.findObjectProvider(pc);
                // Note that this can cause problems like NUCRDBMS-402 due to attempt to re-read the field values
                sm.loadFieldValues(fv); // Load the values retrieved by the query
                return pc;
            }
            if (checkInheritance)
            {
                if (IdentityUtils.isDatastoreIdentity(oid) || IdentityUtils.isSingleFieldIdentity(oid))
                {
                    // Check if this id for any known subclasses is in the cache to save searching
                    String[] subclasses = ec.getMetaDataManager().getSubclassesForClass(pcCls.getName(), true);
                    if (subclasses != null)
                    {
                        for (int i=0;i<subclasses.length;i++)
                        {
                            if (IdentityUtils.isDatastoreIdentity(oid))
                            {
                                oid = ec.getNucleusContext().getIdentityManager().getDatastoreId(subclasses[i], IdentityUtils.getTargetKeyForDatastoreIdentity(oid));
                            }
                            else if (IdentityUtils.isSingleFieldIdentity(oid))
                            {
                                oid = ec.getNucleusContext().getIdentityManager().getSingleFieldId(oid.getClass(),
                                    ec.getClassLoaderResolver().classForName(subclasses[i]), IdentityUtils.getTargetKeyForSingleFieldIdentity(oid));
                            }
                            pc = ec.getObjectFromCache(oid);
                            if (pc != null)
                            {
                                sm = ec.findObjectProvider(pc);
                                sm.loadFieldValues(fv); // Load the values retrieved by the query
                                return pc;
                            }
                        }
                    }
                }
            }
        }

        if (checkInheritance)
        {
            // TODO Remove this reference to ObjectProvider (should be ObjectProvider only)
            sm.checkInheritance(fv); // Find the correct PC class for this object, hence updating the object id
            if (!ignoreCache)
            {
                // Check the cache in case this updated object id is present (since we should use that if available)
                Object oid = sm.getInternalObjectId();
                Object pc = ec.getObjectFromCache(oid);
                if (pc != null)
                {
                    // We have an object with this new object id already so return it with the retrieved field values imposed
                    sm = ec.findObjectProvider(pc);
                    sm.loadFieldValues(fv); // Load the values retrieved by the query
                    return pc;
                }
            }
        }

        // Cache the object as required
        ec.putObjectIntoLevel1Cache(sm);

        return sm.getObject();
    }

    public static Map<LdapName, Attributes> getEntries(StoreManager storeMgr, ExecutionContext ec,
        final AbstractClassMetaData candidateCmd, LdapName base, String additionalFilter, boolean subclasses, boolean ignoreCache)
    {
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            DirContext ctx = (DirContext) mconn.getConnection();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            Map<LdapName, Attributes> results = getEntries(ec, candidateCmd, ctx, base, additionalFilter, ignoreCache);
            if (subclasses)
            {
                // Add on any subclass objects
                String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(candidateCmd.getFullClassName(), true);
                if (subclassNames != null)
                {
                    for (String subclassName : subclassNames)
                    {
                        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(subclassName, clr);
                        results.putAll(getEntries(ec, cmd, ctx, base, additionalFilter, ignoreCache));
                    }
                }
            }
            return results;
        }
        finally
        {
            mconn.release();
        }
    }

    private static Map<LdapName, Attributes> getEntries(ExecutionContext ec, final AbstractClassMetaData acmd, 
        DirContext ctx, LdapName base, String additionalFilter, boolean ignoreCache)
    {
        Map<LdapName, Attributes> results = new LinkedHashMap<LdapName, Attributes>();

        // no results for interface or abstract
        Class c = ec.getClassLoaderResolver().classForName(acmd.getFullClassName());
        if (c.isInterface() || Modifier.isAbstract(c.getModifiers()))
        {
            return results;
        }

        try
        {
            if (base == null)
            {
                base = getSearchBase(acmd, ec.getMetaDataManager());
            }
            String filter = getSearchFilter(acmd);
            if (additionalFilter != null)
            {
                filter = "(&" + filter + additionalFilter + ")";
            }
            SearchControls searchControls = getSearchControls(acmd);

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.search", base, filter, searchControls.getSearchScope()));
            }

            NamingEnumeration<SearchResult> enumeration = ctx.search(base.toString(), filter, searchControls);
            while (enumeration.hasMoreElements())
            {
                final SearchResult sr = enumeration.nextElement();
                final Attributes attrs = sr.getAttributes();
                String name = sr.getNameInNamespace();
                LdapName dn = new LdapName(name);

                if (searchControls.getSearchScope() == SearchControls.SUBTREE_SCOPE)
                {
                    if (dn.equals(base))
                    {
                        // skip this entry, it is the search base
                        continue;
                    }
                }

                results.put(dn, attrs);
            }
        }
        catch (NameNotFoundException nnfe)
        {
            // ignore, occurs when trying to search for an non-existing object
        }
        catch (NamingException ne)
        {
            throw new NucleusDataStoreException(ne.getMessage(), ne);
        }

        return results;
    }

    public static void insert(StoreManager storeMgr, LdapName dn, Attributes attributes, ExecutionContext ec)
    {
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.createSubcontext", dn, attributes));
        }
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            ((DirContext) mconn.getConnection()).bind(dn, null, attributes);
        }
        catch (NamingException ne)
        {
            throw new NucleusDataStoreException(ne.getMessage(), ne);
        }
        finally
        {
            mconn.release();
        }
    }

    public static void update(StoreManager storeMgr, LdapName dn, Attributes attributes, ExecutionContext ec)
    {
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.modifyAttributes", dn, "REPLACE", attributes));
        }
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            ((DirContext) mconn.getConnection()).modifyAttributes(dn, DirContext.REPLACE_ATTRIBUTE, attributes);
        }
        catch (NamingException ne)
        {
            throw new NucleusDataStoreException(ne.getMessage(), ne);
        }
        finally
        {
            mconn.release();
        }
    }

    public static void deleteRecursive(StoreManager storeMgr, LdapName dn, ExecutionContext ec)
    {
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            deleteRecursive(dn, (DirContext) mconn.getConnection());
        }
        catch (NamingException ne)
        {
            throw new NucleusDataStoreException(ne.getMessage(), ne);
        }
        finally
        {
            mconn.release();
        }
    }

    public static void deleteRecursive(LdapName dn, DirContext ctx) throws NamingException
    {
        // search one-level and delete each, on exception: delete all children recursive
        NamingEnumeration<SearchResult> enumeration = ctx.search(dn, "(objectClass=*)", new SearchControls());
        while (enumeration.hasMoreElements())
        {
            SearchResult result = enumeration.nextElement();
            String resultName = result.getNameInNamespace();
            LdapName resultDn = new LdapName(resultName);
            try
            {
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.destroySubcontext", resultDn));
                }
                ctx.unbind(resultDn);
            }
            catch (ContextNotEmptyException cnee)
            {
                deleteRecursive(resultDn, ctx);
            }
        }

        // now all child entries are deleted, delete parent again
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("LDAP.JNDI.destroySubcontext", dn));
        }
        ctx.unbind(dn);
    }
}