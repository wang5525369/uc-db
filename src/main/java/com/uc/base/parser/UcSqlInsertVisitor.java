package com.uc.base.parser;

import com.google.common.collect.Lists;
import com.uc.base.antlr.MySqlParser;
import com.uc.base.sql.UcSqlInsert;
import com.uc.base.sql.UcSqlVarInfo;

import java.util.List;

public class UcSqlInsertVisitor extends UcSqlBaseVisitor {

    private List<UcSqlVarInfo> listInsertTableName = Lists.newArrayList();
    private List<UcSqlVarInfo> listInsertColumnValue = Lists.newArrayList();
    private List<UcSqlVarInfo> listInsertColumnName = Lists.newArrayList();

    private List<UcSqlInsert> listUcSqlInserte = Lists.newArrayList();


    public String visitTableName(MySqlParser.TableNameContext tableNameContext) {
        String tableName = tableNameContext.getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(tableName);
        listInsertTableName.add(ucSqlVarInfo);
        return tableName;
    }

    public List<UcSqlVarInfo> visitInsertStatement(MySqlParser.InsertStatementContext insertStatementContext) {
        List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();

        MySqlParser.TableNameContext tableNameContext = insertStatementContext.tableName();
        visitTableName(tableNameContext);

        List<MySqlParser.UidListContext> listUidListContext = insertStatementContext.uidList();
        visitInsertStatementColumn(listUidListContext);

        MySqlParser.InsertStatementValueContext insertStatementValueContext = insertStatementContext.insertStatementValue();
        visitInsertStatementValue(insertStatementValueContext);

        return listUcSqlVarInfo;
    }

    public List<UcSqlVarInfo> visitInsertStatementColumn(List<MySqlParser.UidListContext> listUidListContext) {
        List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();
        for(MySqlParser.UidListContext  uidListContext : listUidListContext){
            List<MySqlParser.UidContext> listUidContext =  uidListContext.uid();
            for(MySqlParser.UidContext uidContext : listUidContext){
                String columnName = uidContext.getText();
                UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(columnName);
                listInsertColumnName.add(ucSqlVarInfo);
                listUcSqlVarInfo.add(ucSqlVarInfo);
            }
        }
        return listUcSqlVarInfo;
    }

    public List<UcSqlVarInfo> visitInsertStatementValue(MySqlParser.InsertStatementValueContext insertStatementValueContext) {
        List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();
        int i = 0;
        int columnNameSize = listInsertColumnName.size();
        List<MySqlParser.ExpressionsWithDefaultsContext> listExpressionsWithDefaultsContext = insertStatementValueContext.expressionsWithDefaults();
        for(MySqlParser.ExpressionsWithDefaultsContext expressionsWithDefaultsContext : listExpressionsWithDefaultsContext){
            List<MySqlParser.ExpressionOrDefaultContext> listExpressionOrDefaultContext = expressionsWithDefaultsContext.expressionOrDefault();
            for(MySqlParser.ExpressionOrDefaultContext expressionOrDefaultContext : listExpressionOrDefaultContext){
                String value = expressionOrDefaultContext.expression().getText();
                UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(value);
                listInsertColumnValue.add(ucSqlVarInfo);
                listUcSqlVarInfo.add(ucSqlVarInfo);

                UcSqlInsert ucSqlInsert = new UcSqlInsert();
                ucSqlInsert.setInsertColumnValue(ucSqlVarInfo);
                ucSqlInsert.setInsertColumnName(listInsertColumnName.get(i));
                listUcSqlInserte.add(ucSqlInsert);
                if (i>= (columnNameSize-1)){
                    i = 0;
                }else{
                    i++;
                }
            }
        }
        return listUcSqlVarInfo;
    }

    public List<UcSqlVarInfo> getListInsertTableName() {
        return listInsertTableName;
    }

    public List<UcSqlVarInfo> getListInsertColumnValue() {
        return listInsertColumnValue;
    }

    public List<UcSqlVarInfo> getListInsertColumnName() {
        return listInsertColumnName;
    }
}
