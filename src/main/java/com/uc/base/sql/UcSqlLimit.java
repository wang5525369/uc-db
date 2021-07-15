package com.uc.base.sql;

public class UcSqlLimit {
    UcSqlVarInfo offsetVar;
    UcSqlVarInfo sizeVar;

    public UcSqlVarInfo getOffsetVar() {
        return offsetVar;
    }

    public void setOffsetVar(UcSqlVarInfo offsetVar) {
        this.offsetVar = offsetVar;
    }

    public UcSqlVarInfo getSizeVar() {
        return sizeVar;
    }

    public void setSizeVar(UcSqlVarInfo sizeVar) {
        this.sizeVar = sizeVar;
    }
}
