package com.uc.base.sql;

public class UcSqlUpdateInfo {
    private UcSqlVarInfo updateColumnName;
    private UcSqlVarInfo updateColumnValue;

    public UcSqlVarInfo getUpdateColumnName() {
        return updateColumnName;
    }

    public void setUpdateColumnName(UcSqlVarInfo updateColumnName) {
        this.updateColumnName = updateColumnName;
    }

    public UcSqlVarInfo getUpdateColumnValue() {
        return updateColumnValue;
    }

    public void setUpdateColumnValue(UcSqlVarInfo updateColumnValue) {
        this.updateColumnValue = updateColumnValue;
    }
}
