package com.uc.base.utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.function.Function;

public class UcJdbcDateUtils {
    private static final long DAY_IN_MILLIS = 86400000L;
    static final DateTimeFormatter ISO_WITH_MILLIS;

    UcJdbcDateUtils() {
    }

    public static long asMillisSinceEpoch(String date) {
        ZonedDateTime zdt = (ZonedDateTime)ISO_WITH_MILLIS.parse(date, ZonedDateTime::from);
        return zdt.toInstant().toEpochMilli();
    }

    public static Date asDate(String date) {
        return new Date(utcMillisRemoveTime(asMillisSinceEpoch(date)));
    }

    public static Time asTime(String date) {
        return new Time(utcMillisRemoveDate(asMillisSinceEpoch(date)));
    }

    static public Timestamp asTimestamp(String date) {
        return new Timestamp(asMillisSinceEpoch(date));
    }

    public static <R> R asDateTimeField(Object value, Function<String, R> asDateTimeMethod, Function<Long, R> ctor) {
        return value instanceof String ? asDateTimeMethod.apply((String)value) : ctor.apply(((Number)value).longValue());
    }

    private static long utcMillisRemoveTime(long l) {
        return l - l % 86400000L;
    }

    private static long utcMillisRemoveDate(long l) {
        return l % 86400000L;
    }

    static {
        ISO_WITH_MILLIS = (new DateTimeFormatterBuilder()).parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2).appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true).appendOffsetId().toFormatter(Locale.ROOT);
    }
}
