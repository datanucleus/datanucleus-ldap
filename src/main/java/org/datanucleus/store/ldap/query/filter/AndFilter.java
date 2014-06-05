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
 * An AND filter.
 * 
 * Copied and adapted from Apache Directory shared-ldap.
 */
public class AndFilter extends AbstractBranchFilter
{

    /**
     * Instantiates a new AND filter.
     * @param children the child filters
     */
    public AndFilter(Filter... children)
    {
        super(children);
    }

    /**
     * Gets the recursive prefix string representation of this AND filter.
     * @return the string representation of this AND filter
     */
    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        buf.append("(&");
        for (Filter child : getChildren())
        {
            buf.append(child);
        }
        buf.append(')');
        return buf.toString();
    }

}
