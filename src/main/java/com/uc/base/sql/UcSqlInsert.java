package com.uc.base.sql;

public class UcSqlInsert {
    private UcSqlVarInfo insertColumnName;
    private UcSqlVarInfo insertColumnValue;

    public UcSqlVarInfo getInsertColumnName() {
        return insertColumnName;
    }

    public void setInsertColumnName(UcSqlVarInfo insertColumnName) {
        this.insertColumnName = insertColumnName;
    }

    public UcSqlVarInfo getInsertColumnValue() {
        return insertColumnValue;
    }

    public void setInsertColumnValue(UcSqlVarInfo insertColumnValue) {
        this.insertColumnValue = insertColumnValue;
    }
}
