package com.uc.base.parser;

import com.uc.base.sql.UcSqlWhere;

public class SqlTreeNode {
    UcSqlWhere ucSqlWhere;
    SqlTreeNode leftChildSqlTreeNode;
    SqlTreeNode rightChildSqlTreeNode;

    public UcSqlWhere getUcSqlWhere() {
        return ucSqlWhere;
    }

    public void setUcSqlWhere(UcSqlWhere ucSqlWhere) {
        this.ucSqlWhere = ucSqlWhere;
    }

    public SqlTreeNode getLeftChildSqlTreeNode() {
        return leftChildSqlTreeNode;
    }

    public void setLeftChildSqlTreeNode(SqlTreeNode leftChildSqlTreeNode) {
        this.leftChildSqlTreeNode = leftChildSqlTreeNode;
    }

    public SqlTreeNode getRightChildSqlTreeNode() {
        return rightChildSqlTreeNode;
    }

    public void setRightChildSqlTreeNode(SqlTreeNode rightChildSqlTreeNode) {
        this.rightChildSqlTreeNode = rightChildSqlTreeNode;
    }
}
