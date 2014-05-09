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

import java.util.ArrayList;
import java.util.List;

/**
 * An substring filter.
 * 
 * Copied and adapted from Apache Directory shared-ldap.
 */
public class SubstringFilter extends AbstractLeafFilter
{
    /** The initial pattern */
    private String initialPattern;

    /** The final pattern */
    private String finalPattern;

    /** List of patterns */
    private List<String> anyPatterns;

    /**
     * Instantiates a new substring filter.
     * @param attribute the attribute
     */
    public SubstringFilter(String attribute)
    {
        super(attribute);
        this.initialPattern = null;
        this.finalPattern = null;
        this.anyPatterns = new ArrayList<String>(2);
    }

    /**
     * Gets the initial pattern.
     * @return the initial pattern
     */
    public final String getInitialPattern()
    {
        return initialPattern;
    }

    /**
     * Sets the initial pattern.
     * @param initialPattern the initial pattern
     */
    public void setInitialPattern(String initialPattern)
    {
        this.initialPattern = initialPattern;
    }

    /**
     * Gets the final pattern.
     * @return the final pattern
     */
    public final String getFinalPattern()
    {
        return finalPattern;
    }

    /**
     * Sets the final pattern.
     * @param finalPattern the final pattern
     */
    public void setFinalPattern(String finalPattern)
    {
        this.finalPattern = finalPattern;
    }

    /**
     * Gets the list of any patterns.
     * @return the any patterns
     */
    public final List<String> getAnyPatterns()
    {
        return anyPatterns;
    }

    /**
     * Add an any pattern.
     * @param anyPattern the any pattern
     */
    public void addAnyPattern(String anyPattern)
    {
        this.anyPatterns.add(anyPattern);
    }

    /**
     * Gets the string representation of this substring filter.
     * @return the string representation of this substring filter
     */
    public String toString()
    {
        StringBuilder buf = new StringBuilder();

        buf.append('(').append(getAttribute()).append('=');

        if (null != initialPattern)
        {
            buf.append(initialPattern);
        }
        buf.append('*');

        if (null != anyPatterns)
        {
            for (String any : anyPatterns)
            {
                buf.append(any);
                buf.append('*');
            }
        }

        if (null != finalPattern)
        {
            buf.append(finalPattern);
        }

        buf.append(')');

        return buf.toString();
    }
}