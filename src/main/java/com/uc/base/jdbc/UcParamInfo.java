package com.uc.base.jdbc;

import com.uc.base.sql.UcType;


public class UcParamInfo {
    private UcType type;
    private Object value;
    private int sqlType;

    public UcParamInfo(Object value, UcType type, int sqlType) {
        this.value = value;
        this.type = type;
        this.sqlType = sqlType;
    }

    public UcType getType() {
        return type;
    }

    public void setType(UcType type) {
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }
}
