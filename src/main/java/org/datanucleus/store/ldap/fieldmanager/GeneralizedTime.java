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
package org.datanucleus.store.ldap.fieldmanager;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Converts between {@link Calendar} and LDAP generalized time syntax as defined in 
 * RFC 4517 section 3.3.13.
 * 
 * Copied and adapted from Apache Directory shared-ldap.
 */
public class GeneralizedTime implements Comparable<GeneralizedTime>
{

    public enum Format {
        YEAR_MONTH_DAY_HOUR_MIN_SEC, YEAR_MONTH_DAY_HOUR_MIN_SEC_FRACTION,

        YEAR_MONTH_DAY_HOUR_MIN, YEAR_MONTH_DAY_HOUR_MIN_FRACTION,

        YEAR_MONTH_DAY_HOUR, YEAR_MONTH_DAY_HOUR_FRACTION, ;
    }

    public enum FractionDelimiter {
        DOT, COMMA
    }

    public enum TimeZoneFormat {
        Z, DIFF_HOUR, DIFF_HOUR_MINUTE;
    }

    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    /** The user provided value */
    private String upGeneralizedTime;

    /** The user provided format */
    private Format upFormat;

    /** The user provided time zone format */
    private TimeZoneFormat upTimeZoneFormat;

    /** The user provided fraction delimiter */
    private FractionDelimiter upFractionDelimiter;

    /** the user provided fraction length */
    private int upFractionLength;

    /** The calendar */
    private Calendar calendar;

    /**
     * Creates a new instance of GeneralizedTime, based on the given Calendar object. Uses
     * 
     * <pre>
     * Format.YEAR_MONTH_DAY_HOUR_MIN_SEC
     * </pre>
     * 
     * as default format and
     * 
     * <pre>
     * TimeZoneFormat.Z
     * </pre>
     * 
     * as default time zone format.
     * @param calendar the calendar containing the date, time and timezone information
     */
    public GeneralizedTime(Calendar calendar)
    {
        if (calendar == null)
        {
            throw new IllegalArgumentException("Calendar must not be null.");
        }

        this.calendar = calendar;
        upGeneralizedTime = null;
        upFormat = Format.YEAR_MONTH_DAY_HOUR_MIN_SEC;
        upTimeZoneFormat = TimeZoneFormat.Z;
        upFractionDelimiter = FractionDelimiter.DOT;
        upFractionLength = 3;
    }

    /**
     * Creates a new instance of GeneralizedTime, based on the given generalized time string.
     * @param generalizedTime the generalized time
     * @throws ParseException if the given generalized time can't be parsed.
     */
    public GeneralizedTime(String generalizedTime) throws ParseException
    {
        if (generalizedTime == null)
        {
            throw new ParseException("generalizedTime is null", 0);
        }

        this.upGeneralizedTime = generalizedTime;

        calendar = Calendar.getInstance();
        calendar.setTimeInMillis(0);
        calendar.setLenient(false);

        parseYear();
        parseMonth();
        parseDay();
        parseHour();

        if (upGeneralizedTime.length() < 11)
        {
            throw new ParseException("Generalized Time too short, doesn't contain field 'minute' or 'fraction of hour' or 'timezone'.", 10);
        }

        // pos 10:
        // if digit => minute field
        // if . or , => fraction of hour field
        // if Z or + or - => timezone field
        // else error
        int pos = 10;
        char c = upGeneralizedTime.charAt(pos);
        if ('0' <= c && c <= '9')
        {
            parseMinute();

            if (upGeneralizedTime.length() < 13)
            {
                throw new ParseException(
                        "Generalized Time too short, doesn't contain field 'second' or 'fraction of minute' or 'timezone'.", 12);
            }

            // pos 12:
            // if digit => second field
            // if . or , => fraction of minute field
            // if Z or + or - => timezone field
            // else error
            pos = 12;
            c = upGeneralizedTime.charAt(pos);
            if ('0' <= c && c <= '9')
            {
                parseSecond();

                if (upGeneralizedTime.length() < 15)
                {
                    throw new ParseException("Generalized Time too short, doesn't contain field 'fraction of second' or 'timezone'.", 14);
                }

                // pos 14:
                // if . or , => fraction of second field
                // if Z or + or - => timezone field
                // else error
                pos = 14;
                c = upGeneralizedTime.charAt(pos);
                if (c == '.' || c == ',')
                {
                    // read fraction of second
                    parseFractionOfSecond();
                    pos += 1 + upFractionLength;

                    parseTimezone(pos);
                    upFormat = Format.YEAR_MONTH_DAY_HOUR_MIN_SEC_FRACTION;
                }
                else if (c == 'Z' || c == '+' || c == '-')
                {
                    // read timezone
                    parseTimezone(pos);
                    upFormat = Format.YEAR_MONTH_DAY_HOUR_MIN_SEC;
                }
                else
                {
                    throw new ParseException("Invalid Time too short, expected field 'fraction of second' or 'timezone'.", 14);
                }
            }
            else if (c == '.' || c == ',')
            {
                // read fraction of minute
                parseFractionOfMinute();
                pos += 1 + upFractionLength;

                parseTimezone(pos);
                upFormat = Format.YEAR_MONTH_DAY_HOUR_MIN_FRACTION;
            }
            else if (c == 'Z' || c == '+' || c == '-')
            {
                // read timezone
                parseTimezone(pos);
                upFormat = Format.YEAR_MONTH_DAY_HOUR_MIN;
            }
            else
            {
                throw new ParseException("Invalid Time too short, expected field 'second' or 'fraction of minute' or 'timezone'.", 12);
            }
        }
        else if (c == '.' || c == ',')
        {
            // read fraction of hour
            parseFractionOfHour();
            pos += 1 + upFractionLength;

            parseTimezone(pos);
            upFormat = Format.YEAR_MONTH_DAY_HOUR_FRACTION;
        }
        else if (c == 'Z' || c == '+' || c == '-')
        {
            // read timezone
            parseTimezone(pos);
            upFormat = Format.YEAR_MONTH_DAY_HOUR;
        }
        else
        {
            throw new ParseException("Invalid Generalized Time, expected field 'minute' or 'fraction of hour' or 'timezone'.", 10);
        }

        // this calculates and verifies the calendar
        try
        {
            calendar.getTimeInMillis();
        }
        catch (IllegalArgumentException iae)
        {
            throw new ParseException("Invalid date/time values.", 0);
        }

        calendar.setLenient(true);
    }

    private void parseTimezone(int pos) throws ParseException
    {
        if (upGeneralizedTime.length() < pos + 1)
        {
            throw new ParseException("Generalized Time too short, doesn't contain field 'timezone'.", pos);
        }

        char c = upGeneralizedTime.charAt(pos);
        if (c == 'Z')
        {
            calendar.setTimeZone(GMT);
            upTimeZoneFormat = TimeZoneFormat.Z;

            if (upGeneralizedTime.length() > pos + 1)
            {
                throw new ParseException("Invalid Generalized Time, expected 'timezone' as the last field.", pos + 1);
            }
        }
        else if (c == '+' || c == '-')
        {
            StringBuilder sb = new StringBuilder("GMT");
            sb.append(c);

            String digits = getAllDigits(pos + 1);
            sb.append(digits);

            if (digits.length() == 2 && digits.matches("^([01]\\d|2[0-3])$"))
            {
                TimeZone timeZone = TimeZone.getTimeZone(sb.toString());
                calendar.setTimeZone(timeZone);
                upTimeZoneFormat = TimeZoneFormat.DIFF_HOUR;
            }
            else if (digits.length() == 4 && digits.matches("^([01]\\d|2[0-3])([0-5]\\d)$"))
            {
                TimeZone timeZone = TimeZone.getTimeZone(sb.toString());
                calendar.setTimeZone(timeZone);
                upTimeZoneFormat = TimeZoneFormat.DIFF_HOUR_MINUTE;
            }
            else
            {
                throw new ParseException("Invalid Generalized Time, expected field 'timezone' must contain 2 or 4 digits.", pos);
            }

            if (upGeneralizedTime.length() > pos + 1 + digits.length())
            {
                throw new ParseException("Invalid Generalized Time, expected 'timezone' as the last field.", pos + 1 + digits.length());
            }
        }
    }

    private void parseFractionOfSecond() throws ParseException
    {
        parseFractionDelmiter(14);
        String fraction = getFraction(14 + 1);
        upFractionLength = fraction.length();

        double fract = Double.parseDouble("0." + fraction);
        int millisecond = (int) Math.round(fract * 1000);

        calendar.set(Calendar.MILLISECOND, millisecond);
    }

    private void parseFractionOfMinute() throws ParseException
    {
        parseFractionDelmiter(12);
        String fraction = getFraction(12 + 1);
        upFractionLength = fraction.length();

        double fract = Double.parseDouble("0." + fraction);
        int milliseconds = (int) Math.round(fract * 1000 * 60);
        int second = milliseconds / 1000;
        int millisecond = milliseconds - (second * 1000);

        calendar.set(Calendar.SECOND, second);
        calendar.set(Calendar.MILLISECOND, millisecond);
    }

    private void parseFractionOfHour() throws ParseException
    {
        parseFractionDelmiter(10);
        String fraction = getFraction(10 + 1);
        upFractionLength = fraction.length();

        double fract = Double.parseDouble("0." + fraction);
        int milliseconds = (int) Math.round(fract * 1000 * 60 * 60);
        int minute = milliseconds / (1000 * 60);
        int second = (milliseconds - (minute * 60 * 1000)) / 1000;
        int millisecond = milliseconds - (minute * 60 * 1000) - (second * 1000);

        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, second);
        calendar.set(Calendar.MILLISECOND, millisecond);
    }

    private void parseFractionDelmiter(int fractionDelimiterPos)
    {
        char c = upGeneralizedTime.charAt(fractionDelimiterPos);
        upFractionDelimiter = c == '.' ? FractionDelimiter.DOT : FractionDelimiter.COMMA;
    }

    private String getFraction(int startIndex) throws ParseException
    {
        String fraction = getAllDigits(startIndex);

        // minimum one digit
        if (fraction.length() == 0)
        {
            throw new ParseException("Generalized Time too short, doesn't contain number for 'fraction'.", startIndex);
        }

        return fraction;
    }

    private String getAllDigits(int startIndex)
    {
        StringBuilder sb = new StringBuilder();
        while (upGeneralizedTime.length() > startIndex)
        {
            char c = upGeneralizedTime.charAt(startIndex);
            if ('0' <= c && c <= '9')
            {
                sb.append(c);
                startIndex++;
            }
            else
            {
                break;
            }
        }
        return sb.toString();
    }

    private void parseSecond() throws ParseException
    {
        // read minute
        if (upGeneralizedTime.length() < 14)
        {
            throw new ParseException("Generalized Time too short, doesn't contain field 'second'.", 12);
        }
        try
        {
            int second = Integer.parseInt(upGeneralizedTime.substring(12, 14));
            calendar.set(Calendar.SECOND, second);
        }
        catch (NumberFormatException e)
        {
            throw new ParseException("Invalid Generalized Time, field 'second' is not numeric.", 12);
        }
    }

    private void parseMinute() throws ParseException
    {
        // read minute
        if (upGeneralizedTime.length() < 12)
        {
            throw new ParseException("Generalized Time too short, doesn't contain field 'minute'.", 10);
        }
        try
        {
            int minute = Integer.parseInt(upGeneralizedTime.substring(10, 12));
            calendar.set(Calendar.MINUTE, minute);
        }
        catch (NumberFormatException e)
        {
            throw new ParseException("Invalid Generalized Time, field 'minute' is not numeric.", 10);
        }
    }

    private void parseHour() throws ParseException
    {
        if (upGeneralizedTime.length() < 10)
        {
            throw new ParseException("Generalized Time too short, doesn't contain field 'hour'.", 8);
        }
        try
        {
            int hour = Integer.parseInt(upGeneralizedTime.substring(8, 10));
            calendar.set(Calendar.HOUR_OF_DAY, hour);
        }
        catch (NumberFormatException e)
        {
            throw new ParseException("Invalid Generalized Time, field 'hour' is not numeric.", 8);
        }
    }

    private void parseDay() throws ParseException
    {
        if (upGeneralizedTime.length() < 8)
        {
            throw new ParseException("Generalized Time too short, doesn't contain field 'day'.", 6);
        }
        try
        {
            int day = Integer.parseInt(upGeneralizedTime.substring(6, 8));
            calendar.set(Calendar.DAY_OF_MONTH, day);
        }
        catch (NumberFormatException e)
        {
            throw new ParseException("Invalid Generalized Time, field 'day' is not numeric.", 6);
        }
    }

    private void parseMonth() throws ParseException
    {
        if (upGeneralizedTime.length() < 6)
        {
            throw new ParseException("Generalized Time too short, doesn't contain field 'month'.", 4);
        }
        try
        {
            int month = Integer.parseInt(upGeneralizedTime.substring(4, 6));
            calendar.set(Calendar.MONTH, month - 1);
        }
        catch (NumberFormatException e)
        {
            throw new ParseException("Invalid Generalized Time, field 'month' is not numeric.", 4);
        }
    }

    private void parseYear() throws ParseException
    {
        if (upGeneralizedTime.length() < 4)
        {
            throw new ParseException("Generalized Time too short, doesn't contain field 'century/year'.", 0);
        }
        try
        {
            int year = Integer.parseInt(upGeneralizedTime.substring(0, 4));
            calendar.set(Calendar.YEAR, year);
        }
        catch (NumberFormatException e)
        {
            throw new ParseException("Invalid Generalized Time, field 'century/year' is not numeric.", 0);
        }
    }

    /**
     * Returns the string representation of this generalized time. This method uses the same format as the user provided
     * format.
     * @return the string representation of this generalized time
     */
    public String toGeneralizedTime()
    {
        return toGeneralizedTime(upFormat, upFractionDelimiter, upFractionLength, upTimeZoneFormat);
    }

    /**
     * Returns the string representation of this generalized time.
     * @param format the target format
     * @param fractionDelimiter the target fraction delimiter, may be null
     * @param fractionLength the fraction length
     * @param timeZoneFormat the target time zone format
     * @return the string
     */
    public String toGeneralizedTime(Format format, FractionDelimiter fractionDelimiter, int fractionLength, TimeZoneFormat timeZoneFormat)
    {
        Calendar calendar = (Calendar) this.calendar.clone();
        if (timeZoneFormat == TimeZoneFormat.Z)
        {
            calendar.setTimeZone(GMT);
        }

        NumberFormat twoDigits = new DecimalFormat("00");
        NumberFormat fourDigits = new DecimalFormat("00");
        StringBuilder fractionFormat = new StringBuilder("");
        for (int i = 0; i < fractionLength && i < 3; i++)
        {
            fractionFormat.append("0");
        }

        StringBuilder sb = new StringBuilder();
        sb.append(fourDigits.format(calendar.get(Calendar.YEAR)));
        sb.append(twoDigits.format(calendar.get(Calendar.MONTH) + 1));
        sb.append(twoDigits.format(calendar.get(Calendar.DAY_OF_MONTH)));
        sb.append(twoDigits.format(calendar.get(Calendar.HOUR_OF_DAY)));

        switch (format)
        {
            case YEAR_MONTH_DAY_HOUR_MIN_SEC :
                sb.append(twoDigits.format(calendar.get(Calendar.MINUTE)));
                sb.append(twoDigits.format(calendar.get(Calendar.SECOND)));
                break;

            case YEAR_MONTH_DAY_HOUR_MIN_SEC_FRACTION :
                sb.append(twoDigits.format(calendar.get(Calendar.MINUTE)));
                sb.append(twoDigits.format(calendar.get(Calendar.SECOND)));

                NumberFormat fractionDigits = new DecimalFormat(fractionFormat.toString());
                sb.append(fractionDelimiter == FractionDelimiter.COMMA ? ',' : '.');
                sb.append(fractionDigits.format(calendar.get(Calendar.MILLISECOND)));
                break;

            case YEAR_MONTH_DAY_HOUR_MIN :
                sb.append(twoDigits.format(calendar.get(Calendar.MINUTE)));
                break;

            case YEAR_MONTH_DAY_HOUR_MIN_FRACTION :
                sb.append(twoDigits.format(calendar.get(Calendar.MINUTE)));

                // sec + millis => fraction of minute
                double millisec = 1000 * calendar.get(Calendar.SECOND) + calendar.get(Calendar.MILLISECOND);
                double fraction = millisec / (1000 * 60);
                fractionDigits = new DecimalFormat("0." + fractionFormat);
                sb.append(fractionDelimiter == FractionDelimiter.COMMA ? ',' : '.');
                sb.append(fractionDigits.format(fraction).substring(2));
                break;

            case YEAR_MONTH_DAY_HOUR_FRACTION :
                // min + sec + millis => fraction of minute
                millisec = 1000 * 60 * calendar.get(Calendar.MINUTE) + 1000 * calendar.get(Calendar.SECOND) + calendar
                        .get(Calendar.MILLISECOND);
                fraction = millisec / (1000 * 60 * 60);
                fractionDigits = new DecimalFormat("0." + fractionFormat);
                sb.append(fractionDelimiter == FractionDelimiter.COMMA ? ',' : '.');
                sb.append(fractionDigits.format(fraction).substring(2));

                break;
        }

        if (timeZoneFormat == TimeZoneFormat.Z && calendar.getTimeZone().hasSameRules(GMT))
        {
            sb.append('Z');
        }
        else
        {
            TimeZone timeZone = calendar.getTimeZone();
            int rawOffset = timeZone.getRawOffset();
            sb.append(rawOffset < 0 ? '-' : '+');

            rawOffset = Math.abs(rawOffset);
            int hour = rawOffset / (60 * 60 * 1000);
            int minute = (rawOffset - (hour * 60 * 60 * 1000)) / (1000 * 60);

            if (hour < 10)
            {
                sb.append('0');
            }
            sb.append(hour);

            if (timeZoneFormat == TimeZoneFormat.DIFF_HOUR_MINUTE || timeZoneFormat == TimeZoneFormat.Z)
            {
                if (minute < 10)
                {
                    sb.append('0');
                }
                sb.append(minute);
            }
        }

        return sb.toString();
    }

    /**
     * Gets the calendar. It could be used to manipulate this {@link GeneralizedTime} settings.
     * @return the calendar
     */
    public Calendar getCalendar()
    {
        return calendar;
    }

    @Override
    public String toString()
    {
        return toGeneralizedTime();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + calendar.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof GeneralizedTime)
        {
            GeneralizedTime other = (GeneralizedTime) obj;
            return calendar.equals(other.calendar);
        }
        else
        {
            return false;
        }
    }

    /**
     * Compares this GeneralizedTime object with the specified GeneralizedTime object.
     * @param other the other GeneralizedTime object
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the
     * specified object.
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(GeneralizedTime other)
    {
        return calendar.compareTo(other.calendar);
    }

}