package com.uc.base.parser;

import com.google.common.collect.Lists;
import com.uc.base.antlr.MySqlParser;
import com.uc.base.sql.UcSqlVarInfo;
import com.uc.base.sql.UcSqlWhere;

import java.sql.SQLException;
import java.util.List;

public class UcSqlDeleteVisitor extends UcSqlBaseVisitor {
    private List<UcSqlVarInfo> listDeleteTableName = Lists.newArrayList();

    public List<UcSqlWhere> operVisitDeleteStatement(MySqlParser.DeleteStatementContext deleteStatementContext) throws SQLException {
        MySqlParser.SingleDeleteStatementContext singleDeleteStatementContext = deleteStatementContext.singleDeleteStatement();
        if (singleDeleteStatementContext == null){
            throw new SQLException("不支持的sql语句");
        }
        MySqlParser.TableNameContext tableNameContext = singleDeleteStatementContext.tableName();
        visitTableName(tableNameContext);

        MySqlParser.ExpressionContext expressionContext = singleDeleteStatementContext.expression();
        operExpressionContext(expressionContext);

        return listUcSqlWhere;
    }

    public String visitTableName(MySqlParser.TableNameContext tableNameContext) {
        String tableName = tableNameContext.getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(tableName);
        listDeleteTableName.add(ucSqlVarInfo);
        return tableName;
    }

    public List<UcSqlVarInfo> getListDeleteTableName() {
        return listDeleteTableName;
    }
}
