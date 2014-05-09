/**********************************************************************
Copyright (c) 2010 Stefan Seelmann and others. All rights reserved.
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
package org.datanucleus.store.ldap.query.filter;

/**
 * Base implementation for all leaf filter types with an assertion value.
 * 
 * Copied and adapted from Apache Directory shared-ldap.
 */
public abstract class AbstractValueFilter extends AbstractLeafFilter
{
    /** The value of this value filter */
    protected String value;

    /**
     * Instantiates a new value filter.
     * @param attribute the attribute of this value filter
     * @param value the value of this value filter
     */
    protected AbstractValueFilter(String attribute, String value)
    {
        super(attribute);
        this.value = value;
    }

    /**
     * Gets the value.
     * @return the value
     */
    public final String getValue()
    {
        return value;
    }

}
