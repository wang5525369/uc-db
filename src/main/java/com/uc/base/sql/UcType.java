package com.uc.base.sql;

import java.sql.SQLType;
import java.sql.Types;

public enum UcType implements SQLType {
//ES类型
    NULL(Types.NULL,"NULL"),
    UNSUPPORTED(Types.OTHER,"UNSUPPORTED"),
    BOOLEAN(Types.BOOLEAN,"BOOLEAN"),
    BYTE(Types.TINYINT,"BYTE"),
    SHORT(Types.SMALLINT,"SHORT"),
    INTEGER(Types.INTEGER,"INTEGER"),
    LONG(Types.BIGINT,"LONG"),
    DOUBLE(Types.DOUBLE,"DOUBLE"),
    FLOAT(Types.REAL,"FLOAT"),
    HALF_FLOAT(Types.FLOAT,"HALF_FLOAT"),
    SCALED_FLOAT(Types.FLOAT,"SCALED_FLOAT"),
    KEYWORD(Types.VARCHAR,"KEYWORD"),
    TEXT(Types.VARCHAR,"TEXT"),
    OBJECT(Types.JAVA_OBJECT,"OBJECT"),
    NESTED(Types.STRUCT,"NESTED"),
    BINARY(Types.VARBINARY,"BINARY"),
    DATE(Types.TIMESTAMP,"DATE"),
    IP(Types.VARCHAR,"IP"),
    INTERVAL_YEAR(101,"INTERVAL_YEAR"),
    INTERVAL_MONTH(102,"INTERVAL_MONTH"),
    INTERVAL_YEAR_TO_MONTH(107,"INTERVAL_YEAR_TO_MONTH"),
    INTERVAL_DAY(103,"INTERVAL_DAY"),
    INTERVAL_HOUR(104,"INTERVAL_HOUR"),
    INTERVAL_MINUTE(105,"INTERVAL_MINUTE"),
    INTERVAL_SECOND(106,"INTERVAL_SECOND"),
    INTERVAL_DAY_TO_HOUR(108,"INTERVAL_DAY_TO_HOUR"),
    INTERVAL_DAY_TO_MINUTE(109,"INTERVAL_DAY_TO_MINUTE"),
    INTERVAL_DAY_TO_SECOND(110,"INTERVAL_DAY_TO_SECOND"),
    INTERVAL_HOUR_TO_MINUTE(111,"INTERVAL_HOUR_TO_MINUTE"),
    INTERVAL_HOUR_TO_SECOND(112,"INTERVAL_HOUR_TO_SECOND"),
    INTERVAL_MINUTE_TO_SECOND(113,"INTERVAL_MINUTE_TO_SECOND"),
    GEO_POINT(Types.JAVA_OBJECT,"GEO_POINT"),
    COMPLETION(Types.VARCHAR,"COMPLETION"),
//MONGODB类型
    STRING(Types.VARCHAR,"STRING"),
    OBJECTID(Types.JAVA_OBJECT,"OBJECTID"),
    ARRAYLIST(Types.JAVA_OBJECT,"ARRAYLIST"),
    TIMESTAMP(Types.TIMESTAMP,"TIMESTAMP"),
    DOCUMENT(Types.JAVA_OBJECT,"DOCUMENT");

    private Integer type;
    private String name = "";

    private UcType(int type,String name) {
        this.type = type;
        this.name = name;

    }

    public Integer getType() {
        return type;
    }

    public String getName() {
        return this.name;
    }

    public String getVendor() {
        return "org.elasticsearch";
    }

    public Integer getVendorTypeNumber() {
        return this.type;
    }
}
