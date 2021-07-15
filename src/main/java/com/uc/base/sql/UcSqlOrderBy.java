package com.uc.base.sql;

public class UcSqlOrderBy {
    UcSqlVarInfo columnVar;
    UcSqlVarInfo sortVar;

    public UcSqlVarInfo getColumnVar() {
        return columnVar;
    }

    public void setColumnVar(UcSqlVarInfo columnVar) {
        this.columnVar = columnVar;
    }

    public UcSqlVarInfo getSortVar() {
        return sortVar;
    }

    public void setSortVar(UcSqlVarInfo sortVar) {
        this.sortVar = sortVar;
    }
}
