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

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Class to handle the conversion between java.util.Date and the generalized time syntax as described in RFC 4517.
 */
public class DateToGeneralizedTimeStringConverter implements TypeConverter<Date, String>
{
    public Date toMemberType(String str)
    {
        if (str == null)
        {
            return null;
        }

        Calendar cal = null;
        try
        {
            GeneralizedTime gt = new GeneralizedTime(str);
            cal = gt.getCalendar();

            // most LDAP servers only store "century year month day hour minute second"
            // TODO: add extension to store millis as fraction
            cal.set(Calendar.MILLISECOND, 0);
        }
        catch (ParseException e)
        {
            throw new NucleusException("Error parsing string to calendar.", e);
        }

        cal.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = cal.getTime();
        return date;
    }

    public String toDatastoreType(Date date)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        calendar.setTime(date);
        calendar.set(Calendar.MILLISECOND, 0);
        GeneralizedTime gt = new GeneralizedTime(calendar);
        return gt.toGeneralizedTime();
    }
}