/**********************************************************************
Copyright (c) 2008 Stefan Seelmann and others. All rights reserved.
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
package org.datanucleus.store.ldap.query;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.InvokeExpression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.Expression.Operator;
import org.datanucleus.query.symbol.SymbolTable;
import org.datanucleus.store.ldap.LDAPUtils;
import org.datanucleus.store.ldap.query.filter.AndFilter;
import org.datanucleus.store.ldap.query.filter.EqualityFilter;
import org.datanucleus.store.ldap.query.filter.Filter;
import org.datanucleus.store.ldap.query.filter.GreaterEqFilter;
import org.datanucleus.store.ldap.query.filter.LessEqFilter;
import org.datanucleus.store.ldap.query.filter.NotFilter;
import org.datanucleus.store.ldap.query.filter.OrFilter;
import org.datanucleus.store.ldap.query.filter.PresenceFilter;
import org.datanucleus.store.ldap.query.filter.SubstringFilter;

/**
 * Class which maps a compiled query to an LDAP filter. Utilizes the filter of the java query and adds them to the
 * underlying LDAP filter. All other components are not handled here and instead processed by the in-memory evaluator.
 */
public class QueryToLDAPFilterMapper extends AbstractExpressionEvaluator
{
    /** The compilation. */
    QueryCompilation compilation;

    /** Input parameters. */
    Map parameters;

    /** Filter expression. */
    Expression filterExpr;

    /** The class meta data */
    AbstractClassMetaData acmd;

    /** Symbol table for the compiled query. */
    SymbolTable symtbl;

    /** The stack */
    Deque stack = new ArrayDeque();

    /** Map with LDAP attribute types */
    Map ldapAttributeTypeMap;

    /**
     * Constructor.
     * @param compilation The generic query compilation
     * @param parameters Parameters needed
     * @param acmd Metadata for the candidate
     */
    public QueryToLDAPFilterMapper(QueryCompilation compilation, Map parameters, AbstractClassMetaData acmd)
    {
        this.filterExpr = compilation.getExprFilter();
        this.symtbl = compilation.getSymbolTable();
        this.compilation = compilation;
        this.parameters = parameters;
        this.acmd = acmd;
    }

    /**
     * Compiles the query and returns the mapped LDAP filter.
     * @return the mapped LDAP filter or null if no filter
     */
    public String compile()
    {
        // additional filter
        if (filterExpr != null)
        {
            filterExpr.evaluate(this);
            if (!stack.isEmpty())
            {
                Object object = stack.pop();
                if (object instanceof Filter)
                {
                    Filter additionalFilter = (Filter) object;
                    return additionalFilter.toString();
                }

                throw new NucleusException("Unexpected element on stack: object=" + object);
            }

            throw new NucleusException("Unexpected empty stack");
        }

        return null;
    }

    protected Object processOrExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        OrFilter filter;
        if (left instanceof Filter && right instanceof Filter)
        {
            filter = new OrFilter((Filter) left,(Filter) right);
            stack.push(filter);
        }
        else
        {
            // TODO: implement other cases
            throw new NucleusException("Case not handled yet: left=" + left + ", right=" + right);
        }

        return filter;
    }

    protected Object processAndExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        AndFilter filter;
        if (left instanceof Filter && right instanceof Filter)
        {
            filter = new AndFilter((Filter) left, (Filter) right);
            stack.push(filter);
        }
        else
        {
            // TODO: implement other cases
            throw new NucleusException("Case not handled yet: left=" + left + ", right=" + right);
        }

        return filter;
    }

    protected Object processEqExpression(Expression expr)
    {
        return processExpressionWithOperator(expr, Expression.OP_EQ);
    }

    protected Object processNoteqExpression(Expression expr)
    {
        return processExpressionWithOperator(expr, Expression.OP_NOTEQ);
    }

    protected Object processLtExpression(Expression expr)
    {
        return processExpressionWithOperator(expr, Expression.OP_LT);
    }

    protected Object processLteqExpression(Expression expr)
    {
        return processExpressionWithOperator(expr, Expression.OP_LTEQ);
    }

    protected Object processGtExpression(Expression expr)
    {
        return processExpressionWithOperator(expr, Expression.OP_GT);
    }

    protected Object processGteqExpression(Expression expr)
    {
        return processExpressionWithOperator(expr, Expression.OP_GTEQ);
    }

    private Object processExpressionWithOperator(Expression expr, Operator operator)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        Filter filter;
        if (left instanceof PrimaryExpression && right instanceof Literal)
        {
            String param = QueryUtils.getStringValueForExpression((Literal) right, parameters);
            filter = getFilterForPrimaryLiteralValue(operator, (PrimaryExpression) left, param);
            stack.push(filter);
        }
        else if (left instanceof PrimaryExpression && right instanceof String)
        {
            String param = (String) right;
            filter = getFilterForPrimaryLiteralValue(operator, (PrimaryExpression) left, param);
            stack.push(filter);
        }
        else if (left instanceof PrimaryExpression && right instanceof Character)
        {
            String param = ((Character) right).toString();
            filter = getFilterForPrimaryLiteralValue(operator, (PrimaryExpression) left, param);
            stack.push(filter);
        }
        else if (left instanceof PrimaryExpression && right instanceof Number)
        {
            String param = ((Number) right).toString();
            filter = getFilterForPrimaryLiteralValue(operator, (PrimaryExpression) left, param);
            stack.push(filter);
        }
        else
        {
            // TODO: implement other cases
            throw new NucleusException("Case not handled yet: left=" + left + ", right=" + right);
        }

        return filter;
    }

    protected Object processPrimaryExpression(PrimaryExpression expr)
    {
        stack.push(expr);
        return expr;
    }

    protected Object processLiteral(Literal expr)
    {
        stack.push(expr);
        return expr;
    }

    protected Object processParameterExpression(ParameterExpression expr)
    {
        Object value = QueryUtils.getValueForParameterExpression(parameters, expr);
        stack.push(value);
        return value;
    }

    /*
     * Support for startsWith and endsWith
     */
    protected Object processInvokeExpression(InvokeExpression expr)
    {
        Expression invokedExpr = expr.getLeft();
        String method = expr.getOperation();
        SubstringFilter filter;
        if (invokedExpr instanceof PrimaryExpression)
        {
            PrimaryExpression primaryExpression = (PrimaryExpression) invokedExpr;
            String attribute = getLdapAttributeType(primaryExpression);
            if (method.equals("startsWith"))
            {
                // TODO Check if the field we invoke on is String-based
                Expression param = expr.getArguments().get(0);
                String value = QueryUtils.getStringValueForExpression(param, parameters);
                filter = new SubstringFilter(attribute);
                filter.setInitialPattern(getEscapedValue(value));
                stack.push(filter);
            }
            else if (method.equals("endsWith"))
            {
                // TODO Check if the field we invoke on is String-based
                Expression param = expr.getArguments().get(0);
                String value = QueryUtils.getStringValueForExpression(param, parameters);
                filter = new SubstringFilter(attribute);
                filter.setFinalPattern(getEscapedValue(value));
                stack.push(filter);
            }
            else
            {
                // TODO: implement other cases
                throw new NucleusException("Case not handled yet: expr=" + expr);
            }
        }
        else
        {
            // TODO: implement other cases
            throw new NucleusException("Case not handled yet: expr=" + expr);
        }
        return filter;
    }

    private String getEscapedValue(String value)
    {
        /*
         * From RFC 4515: The <valueencoding> rule ensures that the entire filter string is a valid UTF-8 string and
         * provides that the octets that represent the ASCII characters "*" (ASCII 0x2a), "(" (ASCII 0x28), ")" (ASCII
         * 0x29), "\" (ASCII 0x5c), and NUL (ASCII 0x00) are represented as a backslash "\" (ASCII 0x5c) followed by the
         * two hexadecimal digits representing the value of the encoded octet.
         */
        if (value != null)
        {
            value = value.replace("\\", "\\5c");
            value = value.replace("\u0000", "\\00");
            value = value.replace("*", "\\2a");
            value = value.replace("(", "\\28");
            value = value.replace(")", "\\29");
        }

        return value;
    }

    private Filter getFilterForPrimaryLiteralValue(Operator operator, PrimaryExpression expr, String param)
    {
        String attribute = getLdapAttributeType(expr);
        String value = getEscapedValue(param);

        Filter filter = null;
        if (operator == Expression.OP_EQ)
        {
            if (value != null)
            {
                filter = new EqualityFilter(attribute, value);
            }
            else
            {
                // (attribute == null) -> attribute must be absent
                filter = new NotFilter(new PresenceFilter(attribute));
            }
        }
        else if (operator == Expression.OP_NOTEQ)
        {
            if (value != null)
            {
                filter = new NotFilter(new EqualityFilter(attribute, value));
            }
            else
            {
                // (attribute != null) -> attribute must be present
                filter = new PresenceFilter(attribute);
            }
        }
        else if (operator == Expression.OP_LT)
        {
            // LDAP filters doesn't support a pure "lesser than" but only a
            // "lesser than or equal. So we have two possibilities to handle this:
            // 1st: use "lesser than or equal" and let the in-memory evaluator filter the equal ones
            // 2nd: use an AND filter to exclude the equal (&(att<=5)(!(att=5)))
            LessEqFilter lessEqualFilter = new LessEqFilter(attribute, value);
            NotFilter notEqualFilter = new NotFilter(new EqualityFilter(attribute, value));
            filter = new AndFilter(lessEqualFilter, notEqualFilter);
        }
        else if (operator == Expression.OP_LTEQ)
        {
            filter = new LessEqFilter(attribute, value);
        }
        else if (operator == Expression.OP_GT)
        {
            // LDAP filters doesn't support a pure "greater than" but only a
            // "greater than or equal. So we have two possibilities to handle this:
            // 1st: use "greater than or equal" and let the in-memory evaluator filter the equal ones
            // 2nd: use an AND filter to exclude the equal (&(att>=5)(!(att=5)))
            GreaterEqFilter greaterEqualFilter = new GreaterEqFilter(attribute, value);
            NotFilter notEqualFilter = new NotFilter(new EqualityFilter(attribute, value));
            filter = new AndFilter(greaterEqualFilter, notEqualFilter);
        }
        else if (operator == Expression.OP_GTEQ)
        {
            filter = new GreaterEqFilter(attribute, value);
        }
        else
        {
            // TODO: implement other cases
            throw new NucleusException("Case not handled yet: operator=" + operator);
        }

        return filter;
    }

    /**
     * Gets the LDAP attribute type from the given expression.
     * @param expr the expression
     * @return the LDAP attribute type
     * @TODO: is this the right way to get the LDAP attribute type form the expression?
     */
    private String getLdapAttributeType(PrimaryExpression expr)
    {
        String id = null;
        List<String> tuples = expr.getTuples();
        for (String component : tuples)
        {
            if (!component.equals(compilation.getCandidateAlias()))
            {
                id = component;
                break;
            }
        }

        if (ldapAttributeTypeMap == null)
        {
            ldapAttributeTypeMap = new HashMap();

            List<AbstractMemberMetaData> mmds = LDAPUtils.getAllMemberMetaData(acmd);
            for (AbstractMemberMetaData mmd : mmds)
            {
                String ldapAttributeType = LDAPUtils.getAttributeNameForField(mmd);
                ldapAttributeTypeMap.put(mmd.getName(), ldapAttributeType);
            }
        }

        String ldapAttributeType = id;
        if (ldapAttributeTypeMap.containsKey(id))
        {
            ldapAttributeType = (String) ldapAttributeTypeMap.get(id);
        }
        return ldapAttributeType;
    }
}