package com.uc.base.sql;

import com.uc.base.enums.UcSqlEnums;

import java.util.List;

public class UcSqlWhere {
    private UcSqlEnums.SQL_TOKEN sql_token;
    private UcSqlVarInfo letfUcSqlVarInfo;
    private List<UcSqlVarInfo> listRightUcSqlVarInfo;

    public UcSqlWhere(UcSqlEnums.SQL_TOKEN sql_token, UcSqlVarInfo letfUcSqlVarInfo, List<UcSqlVarInfo> listRightUcSqlVarInfo){
        this.sql_token = sql_token;
        this.letfUcSqlVarInfo = letfUcSqlVarInfo;
        this.listRightUcSqlVarInfo = listRightUcSqlVarInfo;
    }

    public UcSqlEnums.SQL_TOKEN getSql_token() {
        return sql_token;
    }

    public void setSql_token(UcSqlEnums.SQL_TOKEN sql_token) {
        this.sql_token = sql_token;
    }

    public UcSqlVarInfo getLetfUcSqlVarInfo() {
        return letfUcSqlVarInfo;
    }

    public void setLetfUcSqlVarInfo(UcSqlVarInfo letfUcSqlVarInfo) {
        this.letfUcSqlVarInfo = letfUcSqlVarInfo;
    }

    public List<UcSqlVarInfo> getListRightUcSqlVarInfo() {
        return listRightUcSqlVarInfo;
    }

    public void setListRightUcSqlVarInfo(List<UcSqlVarInfo> listRightUcSqlVarInfo) {
        this.listRightUcSqlVarInfo = listRightUcSqlVarInfo;
    }
}
