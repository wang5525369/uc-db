package com.uc.base.parser;

import com.google.common.collect.Lists;
import com.uc.base.antlr.MySqlParser;
import com.uc.base.sql.UcSqlUpdateInfo;
import com.uc.base.sql.UcSqlVarInfo;
import com.uc.base.sql.UcSqlWhere;

import java.sql.SQLException;
import java.util.List;

public class UcSqlUpdateVisitor extends UcSqlBaseVisitor {

    private List<UcSqlVarInfo> listUpdateTableName = Lists.newArrayList();
    private List<UcSqlVarInfo> listUpdateColumnValue = Lists.newArrayList();
    private List<UcSqlVarInfo> listUpdateColumnName = Lists.newArrayList();
    private List<UcSqlUpdateInfo> listUcSqlUpdateInfo = Lists.newArrayList();


    public String visitTableName(MySqlParser.TableNameContext tableNameContext) {
        String tableName = tableNameContext.getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(tableName);
        listUpdateTableName.add(ucSqlVarInfo);
        return tableName;
    }

    public List<UcSqlWhere> operVisitUpdateStatement(MySqlParser.UpdateStatementContext updateStatementContext) throws SQLException {
        MySqlParser.SingleUpdateStatementContext singleUpdateStatementContext = updateStatementContext.singleUpdateStatement();
        if (singleUpdateStatementContext == null){
            throw new SQLException("不支持的sql语句");
        }
        MySqlParser.TableNameContext tableNameContext = singleUpdateStatementContext.tableName();
        visitTableName(tableNameContext);

        List<MySqlParser.UpdatedElementContext> listUpdatedElementContext = singleUpdateStatementContext.updatedElement();
        for(MySqlParser.UpdatedElementContext updatedElementContext : listUpdatedElementContext) {
            visitUpdatedElement(updatedElementContext);
        }

        MySqlParser.ExpressionContext expressionContext = singleUpdateStatementContext.expression();
        operExpressionContext(expressionContext);

        return listUcSqlWhere;
    }

    public List<UcSqlUpdateInfo> visitUpdatedElement(MySqlParser.UpdatedElementContext updatedElementContext) {
        UcSqlVarInfo ucSqlVarInfoColumnName = getUpdateColumnName(updatedElementContext);
        listUpdateColumnName.add(ucSqlVarInfoColumnName);
        UcSqlVarInfo ucSqlVarInfoColumnValue = getUpdateColumnValue(updatedElementContext);
        listUpdateColumnValue.add(ucSqlVarInfoColumnValue);
        UcSqlUpdateInfo ucSqlUpdateInfo = new UcSqlUpdateInfo();
        ucSqlUpdateInfo.setUpdateColumnName(ucSqlVarInfoColumnName);
        ucSqlUpdateInfo.setUpdateColumnValue(ucSqlVarInfoColumnValue);
        listUcSqlUpdateInfo.add(ucSqlUpdateInfo);
        return listUcSqlUpdateInfo;
    }

    UcSqlVarInfo getUpdateColumnName(MySqlParser.UpdatedElementContext updatedElementContext){
        MySqlParser.FullColumnNameContext fullColumnNameContext = updatedElementContext.fullColumnName();
        String columnName = fullColumnNameContext.getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(columnName);
        return ucSqlVarInfo;
    }

    UcSqlVarInfo getUpdateColumnValue(MySqlParser.UpdatedElementContext updatedElementContext){
        MySqlParser.ExpressionContext expressionContext = updatedElementContext.expression();
        String columnValue = expressionContext.getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(columnValue);
        return ucSqlVarInfo;
    }

    public List<UcSqlVarInfo> getListUpdateTableName() {
        return listUpdateTableName;
    }

    public List<UcSqlVarInfo> getListUpdateColumnValue() {
        return listUpdateColumnValue;
    }

    public List<UcSqlVarInfo> getListUpdateColumnName() {
        return listUpdateColumnName;
    }

    public List<UcSqlUpdateInfo> getListUcSqlUpdateInfo() {
        return listUcSqlUpdateInfo;
    }
}
