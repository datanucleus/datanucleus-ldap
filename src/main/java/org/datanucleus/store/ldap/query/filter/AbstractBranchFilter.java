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
 * Base implementation for all branch filter types.
 * 
 * Copied and adapted from Apache Directory shared-ldap.
 */
public abstract class AbstractBranchFilter implements Filter
{
    /** The child list of this branch filter */
    protected final List<Filter> children;

    /**
     * Instantiates a new branch filter.
     * @param children the child filters
     */
    protected AbstractBranchFilter(Filter... children)
    {
        this.children = new ArrayList<Filter>(children.length);
        for (Filter child : children)
        {
            this.children.add(child);
        }
    }

    /**
     * {@inheritDoc}
     */
    public final boolean isLeaf()
    {
        return false;
    }

    /**
     * Gets the children below this BranchNode.
     * @return the list of child filters under this branch filter.
     */
    public List<Filter> getChildren()
    {
        return children;
    }

}
