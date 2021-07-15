package com.uc.base.sql;

import com.uc.base.utils.UcJdbcDateUtils;
import com.uc.base.utils.UcTypeUtils;

import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class UcTypeConverter {
    public static final DateTimeFormatter ISO_WITH_MILLIS;

    static {
        ISO_WITH_MILLIS = (new DateTimeFormatterBuilder()).parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2).appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true).appendOffsetId().toFormatter(Locale.ROOT);
    }

    public static <T> T convert(Object o, UcType columnType, Class<T> type, String typeString) throws SQLException {
        if (type == null) {
            return (T) convert(o, columnType, typeString);
        }else{
            if (type.isInstance(o) && UcTypeUtils.classOf(columnType) == type) {
                try {
                    return type.cast(o);
                } catch (ClassCastException ex) {
                    failConversion(o, columnType, typeString, type, ex);
                }
            }
            if (type == String.class) {
                return (T) asString(convert(o, columnType, typeString));
            } else if (type == Boolean.class) {
                return (T) asBoolean(o, columnType, typeString);
            } else if (type == Byte.class) {
                return (T) asByte(o, columnType, typeString);
            } else if (type == Short.class) {
                return (T) asShort(o, columnType, typeString);
            } else if (type == Integer.class) {
                return (T) asInteger(o, columnType, typeString);
            } else if (type == Long.class) {
                return (T) asLong(o, columnType, typeString);
            } else if (type == Float.class) {
                return (T) asFloat(o, columnType, typeString);
            } else if (type == Double.class) {
                return (T) asDouble(o, columnType, typeString);
            } else if (type == Date.class) {
                return (T) asDate(o, columnType, typeString);
            } else if (type == Time.class) {
                return (T) asTime(o, columnType, typeString);
            } else if (type == Timestamp.class) {
                return (T) asTimestamp(o, columnType, typeString);
            } else if (type == byte[].class) {
                return (T) asByteArray(o, columnType, typeString);
            } else if (type == LocalDate.class) {
                return (T) asLocalDate(o, columnType, typeString);
            } else if (type == LocalTime.class) {
                return (T) asLocalTime(o, columnType, typeString);
            } else if (type == LocalDateTime.class) {
                return (T) asLocalDateTime(o, columnType, typeString);
            } else if (type == OffsetTime.class) {
                return (T) asOffsetTime(o, columnType, typeString);
            } else {
                return type == OffsetDateTime.class ? (T) asOffsetDateTime(o, columnType, typeString) : failConversion(o, columnType, typeString, type);
            }
        }
    }

    public static <T> T failConversion(Object value, UcType columnType, String typeString, Class<T> target, Exception e) throws SQLException {
        String message = String.format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to [%s]", value, columnType, typeString);
        throw e != null ? new SQLException(message, e) : new SQLException(message);
    }

    public static Object convert(Object o, UcType columnType, String typeString) throws SQLException {
        switch(columnType) {
            case NULL:
                return null;
            case STRING:
            case OBJECTID:
            case ARRAYLIST:
            case TIMESTAMP:
            case DOCUMENT:
            case BOOLEAN:
            case TEXT:
            case KEYWORD:
            case NESTED:
                return o;
            case BYTE:
                return ((Number)o).byteValue();
            case SHORT:
                return ((Number)o).shortValue();
            case INTEGER:
                return ((Number)o).intValue();
            case LONG:
                return ((Number)o).longValue();
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
                return doubleValue(o);
            case FLOAT:
                return floatValue(o);
            case DATE:
                return UcJdbcDateUtils.asDateTimeField(o, UcJdbcDateUtils::asTimestamp, Timestamp::new);
            case INTERVAL_YEAR:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_TO_MONTH:
                return Period.parse(o.toString());
            case INTERVAL_DAY:
            case INTERVAL_HOUR:
            case INTERVAL_MINUTE:
            case INTERVAL_SECOND:
            case INTERVAL_DAY_TO_HOUR:
            case INTERVAL_DAY_TO_MINUTE:
            case INTERVAL_DAY_TO_SECOND:
            case INTERVAL_HOUR_TO_MINUTE:
            case INTERVAL_HOUR_TO_SECOND:
            case INTERVAL_MINUTE_TO_SECOND:
                return Duration.parse(o.toString());
            default:
                throw new SQLException("Unexpected column type [" + typeString + "]");
        }
    }

    public static Float floatValue(Object o) {
        if (o instanceof String) {
            String stringObject = (String)o;
            byte b = -1;
            switch(stringObject.hashCode()) {
                case 78043:
                    if (stringObject.equals("NaN")) {
                        b = 0;
                    }
                    break;
                case 237817416:
                    if (stringObject.equals("Infinity")) {
                        b = 1;
                    }
                    break;
                case 506745205:
                    if (stringObject.equals("-Infinity")) {
                        b = 2;
                    }
            }

            switch(b) {
                case 0:
                    return (float)(0.0F / 0.0);
                case 1:
                    return (float)(1.0F / 0.0);
                case 2:
                    return (float)(-1.0F / 0.0);
                default:
                    return Float.parseFloat((String)o);
            }
        } else {
            return ((Number)o).floatValue();
        }
    }

    public static Double doubleValue(Object o) {
        if (o instanceof String) {
            String stringObject = (String)o;
            byte b = -1;
            switch(stringObject.hashCode()) {
                case 78043:
                    if (stringObject.equals("NaN")) {
                        b = 0;
                    }
                    break;
                case 237817416:
                    if (stringObject.equals("Infinity")) {
                        b = 1;
                    }
                    break;
                case 506745205:
                    if (stringObject.equals("-Infinity")) {
                        b = 2;
                    }
            }

            switch(b) {
                case 0:
                    return 0.0D / 0.0;
                case 1:
                    return 1.0D / 0.0;
                case 2:
                    return -1.0D / 0.0;
                default:
                    return Double.parseDouble((String)o);
            }
        } else {
            return ((Number)o).doubleValue();
        }
    }

    public static String asString(Object nativeValue) {
        return nativeValue == null ? null : toString(nativeValue);
    }

    public static String toString(Object o) {
        if (o == null) {
            return "null";
        } else if (o instanceof Timestamp) {
            Timestamp ts = (Timestamp)o;
            return ts.toInstant().toString();
        } else if (o instanceof ZonedDateTime) {
            return ((ZonedDateTime)o).format(ISO_WITH_MILLIS);
        } else {
            StringBuilder sb;
            if (o instanceof Period) {
                sb = new StringBuilder(7);
                Period p = (Period)o;
                if (p.isNegative()) {
                    sb.append("-");
                    p = p.negated();
                } else {
                    sb.append("+");
                }

                sb.append(p.getYears());
                sb.append("-");
                sb.append(p.getMonths());
                return sb.toString();
            } else if (o instanceof Duration) {
                sb = new StringBuilder(23);
                Duration d = (Duration)o;
                if (d.isNegative()) {
                    sb.append("-");
                    d = d.negated();
                } else {
                    sb.append("+");
                }

                long durationInSec = d.getSeconds();
                sb.append(durationInSec / 86400L);
                sb.append(" ");
                durationInSec %= 86400L;
                sb.append(indent(durationInSec / 3600L));
                sb.append(":");
                durationInSec %= 3600L;
                sb.append(indent(durationInSec / 60L));
                sb.append(":");
                durationInSec %= 60L;
                sb.append(indent(durationInSec));
                sb.append(".");
                sb.append(TimeUnit.NANOSECONDS.toMillis((long)d.getNano()));
                return sb.toString();
            } else {
                return Objects.toString(o);
            }
        }
    }

    public static String indent(long timeUnit) {
        return timeUnit < 10L ? "0" + timeUnit : Long.toString(timeUnit);
    }

    public static Boolean asBoolean(Object o, UcType columnType, String typeString) throws SQLException {
        switch(columnType) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return Integer.signum(((Number)o).intValue()) != 0;
            case TEXT:
            case KEYWORD:
                return Boolean.valueOf((String)o);
            default:
                return failConversion(o, columnType, typeString, Boolean.class);
        }
    }

    public static <T> T failConversion(Object value, UcType columnType, String typeString, Class<T> target) throws SQLException {
        return failConversion(value, columnType, typeString, target, (Exception)null);
    }

    public static Byte asByte(Object o, UcType columnType, String typeString) throws SQLException {
        switch(columnType) {
            case BOOLEAN:
                return Byte.valueOf((byte)((Boolean)o ? 1 : 0));
            case TEXT:
            case KEYWORD:
                try {
                    return Byte.valueOf((String)o);
                } catch (NumberFormatException ex) {
                    return failConversion(o, columnType, typeString, Byte.class, ex);
                }
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return safeToByte(((Number)o).longValue());
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return safeToByte(safeToLong(((Number)o).doubleValue()));
            default:
                return failConversion(o, columnType, typeString, Byte.class);
        }
    }

    public static byte safeToByte(long x) throws SQLException {
        if (x <= 127L && x >= -128L) {
            return (byte)((int)x);
        } else {
            throw new SQLException(String.format(Locale.ROOT, "Numeric %s out of range", Long.toString(x)));
        }
    }

    public static long safeToLong(double x) throws SQLException {
        if (x <= 9.223372036854776E18D && x >= -9.223372036854776E18D) {
            return Math.round(x);
        } else {
            throw new SQLException(String.format(Locale.ROOT, "Numeric %s out of range", Double.toString(x)));
        }
    }

    public static Short asShort(Object o, UcType columnType, String typeString) throws SQLException {
        switch(columnType) {
            case BOOLEAN:
                return Short.valueOf((short)((Boolean)o ? 1 : 0));
            case TEXT:
            case KEYWORD:
                try {
                    return Short.valueOf((String)o);
                } catch (NumberFormatException ex) {
                    return (Short)failConversion(o, columnType, typeString, Short.class, ex);
                }
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return safeToShort(((Number)o).longValue());
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return safeToShort(safeToLong(((Number)o).doubleValue()));
            default:
                return failConversion(o, columnType, typeString, Short.class);
        }
    }

    public static short safeToShort(long x) throws SQLException {
        if (x <= 32767L && x >= -32768L) {
            return (short)((int)x);
        } else {
            throw new SQLException(String.format(Locale.ROOT, "Numeric %s out of range", Long.toString(x)));
        }
    }

    public static Integer asInteger(Object o, UcType columnType, String typeString) throws SQLException {
        switch(columnType) {
            case BOOLEAN:
                return (Boolean)o ? 1 : 0;
            case TEXT:
            case KEYWORD:
                try {
                    return Integer.valueOf((String)o);
                } catch (NumberFormatException ex) {
                    return (Integer)failConversion(o, columnType, typeString, Integer.class, ex);
                }
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return safeToInt(((Number)o).longValue());
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return safeToInt(safeToLong(((Number)o).doubleValue()));
            default:
                return failConversion(o, columnType, typeString, Integer.class);
        }
    }

    public static int safeToInt(long x) throws SQLException {
        if (x <= 2147483647L && x >= -2147483648L) {
            return (int)x;
        } else {
            throw new SQLException(String.format(Locale.ROOT, "Numeric %s out of range", Long.toString(x)));
        }
    }

    public static Long asLong(Object o, UcType columnType, String typeString) throws SQLException {
        switch(columnType) {
            case BOOLEAN:
                return (Boolean)o ? 1L : 0L;
            case TEXT:
            case KEYWORD:
                try {
                    return Long.valueOf((String)o);
                } catch (NumberFormatException ex) {
                    return (Long)failConversion(o, columnType, typeString, Long.class, ex);
                }
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return ((Number)o).longValue();
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return safeToLong(((Number)o).doubleValue());
            default:
                return (Long)failConversion(o, columnType, typeString, Long.class);
        }
    }

    public static Float asFloat(Object o, UcType columnType, String typeString) throws SQLException {
        switch(columnType) {
            case BOOLEAN:
                return (Boolean)o ? 1.0F : 0.0F;
            case TEXT:
            case KEYWORD:
                try {
                    return Float.valueOf((String)o);
                } catch (NumberFormatException ex) {
                    return (Float)failConversion(o, columnType, typeString, Float.class, ex);
                }
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return (float)((Number)o).longValue();
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return ((Number)o).floatValue();
            default:
                return failConversion(o, columnType, typeString, Float.class);
        }
    }

    public static Double asDouble(Object o, UcType columnType, String typeString) throws SQLException {
        switch(columnType) {
            case BOOLEAN:
                return (Boolean)o ? 1.0D : 0.0D;
            case TEXT:
            case KEYWORD:
                try {
                    return Double.valueOf((String)o);
                } catch (NumberFormatException ex) {
                    return (Double)failConversion(o, columnType, typeString, Double.class, ex);
                }
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return (double)((Number)o).longValue();
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return ((Number)o).doubleValue();
            default:
                return (Double)failConversion(o, columnType, typeString, Double.class);
        }
    }

    public static Date asDate(Object o, UcType columnType, String typeString) throws SQLException {
        return columnType == UcType.DATE ? (Date) UcJdbcDateUtils.asDateTimeField(o, UcJdbcDateUtils::asDate, Date::new) : (Date)failConversion(o, columnType, typeString, Date.class);
    }

    public static Time asTime(Object o, UcType columnType, String typeString) throws SQLException {
        return columnType == UcType.DATE ? (Time)UcJdbcDateUtils.asDateTimeField(o, UcJdbcDateUtils::asTime, Time::new) : (Time)failConversion(o, columnType, typeString, Time.class);
    }

    public static Timestamp asTimestamp(Object o, UcType columnType, String typeString) throws SQLException {
        return columnType == UcType.DATE ? (Timestamp)UcJdbcDateUtils.asDateTimeField(o, UcJdbcDateUtils::asTimestamp, Timestamp::new) : (Timestamp)failConversion(o, columnType, typeString, Timestamp.class);
    }

    public static byte[] asByteArray(Object o, UcType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public static LocalDate asLocalDate(Object o, UcType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public static LocalTime asLocalTime(Object o, UcType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public static LocalDateTime asLocalDateTime(Object o, UcType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public static OffsetTime asOffsetTime(Object o, UcType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public static OffsetDateTime asOffsetDateTime(Object o, UcType columnType, String typeString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public static Date convertDate(Long millis, Calendar cal) {
        return (Date)dateTimeConvert(millis, cal, (c) -> {
            c.set(11, 0);
            c.set(12, 0);
            c.set(13, 0);
            c.set(14, 0);
            return new Date(c.getTimeInMillis());
        });
    }

    public static <T> T dateTimeConvert(Long millis, Calendar c, Function<Calendar, T> creator) {
        if (millis == null) {
            return null;
        } else {
            long initial = c.getTimeInMillis();

            Object o;
            try {
                c.setTimeInMillis(millis);
                o = creator.apply(c);
            } finally {
                c.setTimeInMillis(initial);
            }

            return (T) o;
        }
    }

    public static Time convertTime(Long millis, Calendar cal) {
        return (Time)dateTimeConvert(millis, cal, (c) -> {
            c.set(0, 1);
            c.set(1, 1970);
            c.set(2, 0);
            c.set(5, 1);
            return new Time(c.getTimeInMillis());
        });
    }

    public static Timestamp convertTimestamp(Long millis, Calendar cal) {
        return (Timestamp)dateTimeConvert(millis, cal, (c) -> {
            return new Timestamp(c.getTimeInMillis());
        });
    }

    public static long convertFromCalendarToUTC(long value, Calendar cal) {
        if (cal == null) {
            return value;
        } else {
            Calendar c = (Calendar)cal.clone();
            c.setTimeInMillis(value);
            ZonedDateTime convertedDateTime = ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId()).withZoneSameLocal(ZoneOffset.UTC);
            return convertedDateTime.toInstant().toEpochMilli();
        }
    }
}
