package com.uc.base.utils;


import com.uc.base.sql.UcType;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Date;
import java.util.*;

public class UcTypeUtils {
    private static final Map<Class<?>, UcType> CLASS_TO_TYPE;
    private static final Map<UcType, Class<?>> TYPE_TO_CLASS;
    private static final Map<String, UcType> ENUM_NAME_TO_TYPE;
    private static final Map<Integer, UcType> SQL_TO_TYPE;
    private static final Set<UcType> SIGNED_TYPE;


    public static boolean isSigned(UcType type) {
        return SIGNED_TYPE.contains(type);
    }

    public static Class<?> classOf(UcType type) {
        return (Class)TYPE_TO_CLASS.get(type);
    }

    public static SQLType asSqlType(int sqlType) throws SQLException {
        JDBCType[] jdbcTypes = (JDBCType[])JDBCType.class.getEnumConstants();
        int size = jdbcTypes.length;

        for(int i = 0; i < size; ++i) {
            JDBCType jdbcType = jdbcTypes[i];
            if (sqlType == jdbcType.getVendorTypeNumber()) {
                return jdbcType;
            }
        }

        return of(sqlType);
    }

    public static UcType of(SQLType sqlType) throws SQLException {
        if (sqlType instanceof UcType) {
            return (UcType)sqlType;
        } else {
            UcType dataType = (UcType)SQL_TO_TYPE.get(sqlType.getVendorTypeNumber());
            if (dataType == null) {
                throw new SQLFeatureNotSupportedException("Unsupported SQL type [" + sqlType + "]");
            } else {
                return dataType;
            }
        }
    }

    public static UcType of(int sqlType) throws SQLException {
        UcType dataType = (UcType)SQL_TO_TYPE.get(sqlType);
        if (dataType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported SQL type [" + sqlType + "]");
        } else {
            return dataType;
        }
    }

    public static UcType of(String name) throws SQLException {
        UcType dataType = (UcType)ENUM_NAME_TO_TYPE.get(name);
        if (dataType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported Data type [" + name + "]");
        } else {
            return dataType;
        }
    }

    public static boolean isString(UcType dataType) {
        return dataType == UcType.KEYWORD || dataType == UcType.TEXT;
    }

    public static UcType of(Class<? extends Object> clazz) throws SQLException {
        UcType dataType = (UcType)CLASS_TO_TYPE.get(clazz);
        if (dataType == null) {
            Iterator var2 = CLASS_TO_TYPE.entrySet().iterator();

            Map.Entry e;
            do {
                if (!var2.hasNext()) {
                    throw new SQLFeatureNotSupportedException("Objects of type [" + clazz.getName() + "] are not supported");
                }

                e = (Map.Entry)var2.next();
            } while(!((Class)e.getKey()).isAssignableFrom(clazz));

            return (UcType)e.getValue();
        } else {
            return dataType;
        }
    }

    static {
        SIGNED_TYPE = EnumSet.of(UcType.BYTE, UcType.SHORT, UcType.INTEGER, UcType.LONG, UcType.FLOAT, UcType.HALF_FLOAT, UcType.SCALED_FLOAT, UcType.DOUBLE, UcType.DATE);

        Map<Class<?>, UcType> aMap = new LinkedHashMap();
        aMap.put(Boolean.class, UcType.BOOLEAN);
        aMap.put(Byte.class, UcType.BYTE);
        aMap.put(Short.class, UcType.SHORT);
        aMap.put(Integer.class, UcType.INTEGER);
        aMap.put(Long.class, UcType.LONG);
        aMap.put(Float.class, UcType.FLOAT);
        aMap.put(Double.class, UcType.DOUBLE);
        aMap.put(String.class, UcType.KEYWORD);
        aMap.put(byte[].class, UcType.BINARY);
        aMap.put(String.class, UcType.KEYWORD);
        aMap.put(Timestamp.class, UcType.DATE);
        aMap.put(Calendar.class, UcType.DATE);
        aMap.put(GregorianCalendar.class, UcType.DATE);
        aMap.put(Date.class, UcType.DATE);
        aMap.put(java.sql.Date.class, UcType.DATE);
        aMap.put(Time.class, UcType.DATE);
        aMap.put(LocalDateTime.class, UcType.DATE);
        aMap.put(String.class, UcType.STRING);
        aMap.put(Object.class, UcType.OBJECTID);
        aMap.put(Object.class, UcType.NESTED);
        aMap.put(Object.class, UcType.ARRAYLIST);
        aMap.put(Timestamp.class, UcType.TIMESTAMP);
        aMap.put(Object.class, UcType.DOCUMENT);
        CLASS_TO_TYPE = Collections.unmodifiableMap(aMap);

        Map<UcType, Class<?>> types = new LinkedHashMap();
        types.put(UcType.BOOLEAN, Boolean.class);
        types.put(UcType.BYTE, Byte.class);
        types.put(UcType.SHORT, Short.class);
        types.put(UcType.INTEGER, Integer.class);
        types.put(UcType.LONG, Long.class);
        types.put(UcType.DOUBLE, Double.class);
        types.put(UcType.FLOAT, Float.class);
        types.put(UcType.HALF_FLOAT, Double.class);
        types.put(UcType.SCALED_FLOAT, Double.class);
        types.put(UcType.KEYWORD, String.class);
        types.put(UcType.TEXT, String.class);
        types.put(UcType.BINARY, byte[].class);
        types.put(UcType.DATE, Timestamp.class);
        types.put(UcType.IP, String.class);
        types.put(UcType.INTERVAL_YEAR, Period.class);
        types.put(UcType.INTERVAL_MONTH, Period.class);
        types.put(UcType.INTERVAL_YEAR_TO_MONTH, Period.class);
        types.put(UcType.INTERVAL_DAY, Duration.class);
        types.put(UcType.INTERVAL_HOUR, Duration.class);
        types.put(UcType.INTERVAL_MINUTE, Duration.class);
        types.put(UcType.INTERVAL_SECOND, Duration.class);
        types.put(UcType.INTERVAL_DAY_TO_HOUR, Duration.class);
        types.put(UcType.INTERVAL_DAY_TO_MINUTE, Duration.class);
        types.put(UcType.INTERVAL_DAY_TO_SECOND, Duration.class);
        types.put(UcType.INTERVAL_HOUR_TO_MINUTE, Duration.class);
        types.put(UcType.INTERVAL_HOUR_TO_SECOND, Duration.class);
        types.put(UcType.INTERVAL_MINUTE_TO_SECOND, Duration.class);
        types.put(UcType.STRING,String.class);
        types.put(UcType.OBJECTID,Object.class);
        types.put(UcType.NESTED,Object.class);
        types.put(UcType.ARRAYLIST, Object.class);
        types.put(UcType.TIMESTAMP, Timestamp.class);
        types.put(UcType.DOCUMENT, Object.class);
        TYPE_TO_CLASS = Collections.unmodifiableMap(types);

        Map<String, UcType> strings = new LinkedHashMap();
        Map<Integer, UcType> numbers = new LinkedHashMap();
        UcType[] esTypes = UcType.values();
        int size = esTypes.length;

        for(int i = 0; i < size; ++i) {
            UcType dataType = esTypes[i];
            strings.put(dataType.getName().toLowerCase(Locale.ROOT), dataType);
            numbers.putIfAbsent(dataType.getVendorTypeNumber(), dataType);
        }

        ENUM_NAME_TO_TYPE = Collections.unmodifiableMap(strings);
        SQL_TO_TYPE = Collections.unmodifiableMap(numbers);
    }
}
