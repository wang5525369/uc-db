package com.uc.base.sql;

public class UcSqlVarInfo {
    private int varIndex = 0;
    private VarType varType;
    private String varValue = null;

    public enum VarType{
        CONSTANT,VARIABLE
    }

    public UcSqlVarInfo(String varValue, VarType varType, int varIndex){
        this.varIndex = varIndex;
        this.varValue = varValue;
        this.varType = varType;
    }

    public int getVarIndex() {
        return varIndex;
    }

    public void setVarIndex(int varIndex) {
        this.varIndex = varIndex;
    }

    public VarType getVarType() {
        return varType;
    }

    public void setVarType(VarType varType) {
        this.varType = varType;
    }

    public String getVarValue() {
        return varValue;
    }

    public void setVarValue(String varValue) {
        this.varValue = varValue;
    }
}
