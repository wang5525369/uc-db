package com.uc.base.sql;

public class UcSqlHighlight {
    private UcSqlVarInfo letfUcSqlVarInfo;
    private UcSqlVarInfo rightUcSqlVarInfo;

    public UcSqlHighlight(UcSqlVarInfo letfUcSqlVarInfo, UcSqlVarInfo rightUcSqlVarInfo){
        this.letfUcSqlVarInfo = letfUcSqlVarInfo;
        this.rightUcSqlVarInfo = rightUcSqlVarInfo;
    }

    public UcSqlVarInfo getLetfUcSqlVarInfo() {
        return letfUcSqlVarInfo;
    }

    public void setLetfUcSqlVarInfo(UcSqlVarInfo letfUcSqlVarInfo) {
        this.letfUcSqlVarInfo = letfUcSqlVarInfo;
    }

    public UcSqlVarInfo getRightUcSqlVarInfo() {
        return rightUcSqlVarInfo;
    }

    public void setRightUcSqlVarInfo(UcSqlVarInfo rightUcSqlVarInfo) {
        this.rightUcSqlVarInfo = rightUcSqlVarInfo;
    }
}
