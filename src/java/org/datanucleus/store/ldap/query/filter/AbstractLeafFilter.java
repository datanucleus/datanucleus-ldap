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
 * Base implementation for all leaf filter types.
 * 
 * Copied and adapted from Apache Directory shared-ldap.
 */
public abstract class AbstractLeafFilter implements Filter
{

    /** The attribute of this leaf filter */
    protected String attribute;

    /**
     * Instantiates a new leaf filter.
     * @param attribute the attribute of this leaf filter
     */
    protected AbstractLeafFilter(String attribute)
    {
        this.attribute = attribute;
    }

    /**
     * {@inheritDoc}
     */
    public final boolean isLeaf()
    {
        return true;
    }

    /**
     * Gets the attribute of this leaf filter.
     * @return the attribute asserted
     */
    public final String getAttribute()
    {
        return attribute;
    }

}
