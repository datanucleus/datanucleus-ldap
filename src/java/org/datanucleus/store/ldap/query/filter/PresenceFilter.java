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
 * An presence filter.
 * 
 * Copied and adapted from Apache Directory shared-ldap.
 */
public final class PresenceFilter extends AbstractLeafFilter
{
    /**
     * Instantiates a new presence filter.
     * @param attribute the attribute
     */
    public PresenceFilter(String attribute)
    {
        super(attribute);
    }

    /**
     * Gets the string representation of this presence filter.
     * @return the string representation of this presence filter
     */
    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        buf.append('(').append(getAttribute()).append("=*").append(')');
        return buf.toString();
    }
}
