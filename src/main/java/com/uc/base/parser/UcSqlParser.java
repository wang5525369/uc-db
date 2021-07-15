package com.uc.base.parser;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.uc.base.antlr.MySqlLexer;
import com.uc.base.antlr.MySqlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;

import java.sql.SQLException;

public class UcSqlParser {

    public String errorMsg = "";

    public enum DmlType{
        INSERT,UPDATE,DELETE,SELECT;
    }

    public DmlType dmlType;

    private UcSqlInsertVisitor ucSqlInsertVisitor;
    private UcSqlUpdateVisitor ucSqlUpdateVisitor;
    private UcSqlSelectVisitor ucSqlSelectVisitor;
    private UcSqlDeleteVisitor ucSqlDeleteVisitor;
    private String sql;

    static Cache<String, UcSqlParser> cacheUcSqlParser = CacheBuilder.newBuilder().concurrencyLevel(10)
            .maximumSize(100)
            .build();

    public boolean parserSql(String sql) throws SQLException {
        boolean bRet = false;
        errorMsg = "";
        sql = sql.replace("\n"," ");
        sql = sql.replace("\r"," ");

        this.sql = sql;

        UcSqlParser ucSqlParser = cacheUcSqlParser.asMap().get(sql);
        if (ucSqlParser != null) {
            this.dmlType = ucSqlParser.getDmlType();
            this.ucSqlDeleteVisitor = ucSqlParser.getUcSqlDeleteVisitor();
            this.ucSqlInsertVisitor = ucSqlParser.getUcSqlInsertVisitor();
            this.ucSqlSelectVisitor = ucSqlParser.getUcSqlSelectVisitor();
            this.ucSqlUpdateVisitor = ucSqlParser.getUcSqlUpdateVisitor();
        }
        else {
            CodePointCharStream charStreams = CharStreams.fromString(sql);
            UcCaseChangingCharStream ucCaseChangingCharStream = new UcCaseChangingCharStream(charStreams, true);
            MySqlLexer mySqlLexer = new MySqlLexer(ucCaseChangingCharStream);
            MySqlParser mySqlParser = new MySqlParser(new CommonTokenStream(mySqlLexer));
            UcConsoleErrorListener ucConsoleErrorListener = new UcConsoleErrorListener();
            mySqlParser.addErrorListener(ucConsoleErrorListener);

            MySqlParser.DmlStatementContext dmlStatementContext = mySqlParser.dmlStatement();


            if (mySqlParser.getNumberOfSyntaxErrors() > 0) {
                errorMsg = ucConsoleErrorListener.getErrorMsg();
                throw new SQLException(errorMsg);
            }

            MySqlParser.SelectStatementContext selectStatementContext = dmlStatementContext.selectStatement();
            parseSelectVisitor(selectStatementContext);

            MySqlParser.InsertStatementContext insertStatementContext = dmlStatementContext.insertStatement();
            parseInsertVisitor(insertStatementContext);

            MySqlParser.UpdateStatementContext updateStatementContext = dmlStatementContext.updateStatement();
            parseUpdateVisitor(updateStatementContext);

            MySqlParser.DeleteStatementContext deleteStatementContext = dmlStatementContext.deleteStatement();
            parseDeleteVisitor(deleteStatementContext);

            cacheUcSqlParser.put(sql,this);
        }
        return bRet = true;
    }

    void parseInsertVisitor(MySqlParser.InsertStatementContext insertStatementContext){
        if (insertStatementContext != null) {
            ucSqlInsertVisitor = new UcSqlInsertVisitor();
            insertStatementContext.accept(ucSqlInsertVisitor);
            dmlType = DmlType.INSERT;
        }
    }

    void parseUpdateVisitor(MySqlParser.UpdateStatementContext  updateStatementContext) throws SQLException {
        if (updateStatementContext != null) {
            ucSqlUpdateVisitor = new UcSqlUpdateVisitor();
            //updateStatementContext.accept(ucSqlUpdateVisitor);
            ucSqlUpdateVisitor.operVisitUpdateStatement(updateStatementContext);
            dmlType = DmlType.UPDATE;
        }
    }

    void parseSelectVisitor(MySqlParser.SelectStatementContext selectStatementContext) throws SQLException {
        if (selectStatementContext != null) {
            ucSqlSelectVisitor = new UcSqlSelectVisitor();
            selectStatementContext.accept(ucSqlSelectVisitor);
            dmlType = DmlType.SELECT;
        }
    }

    void parseDeleteVisitor(MySqlParser.DeleteStatementContext deleteStatementContext) throws SQLException {
        if (deleteStatementContext != null) {
            ucSqlDeleteVisitor = new UcSqlDeleteVisitor();
            ucSqlDeleteVisitor.operVisitDeleteStatement(deleteStatementContext);
            dmlType = DmlType.DELETE;
        }
    }

    public UcSqlInsertVisitor getUcSqlInsertVisitor() {
        return ucSqlInsertVisitor;
    }

    public UcSqlUpdateVisitor getUcSqlUpdateVisitor() {
        return ucSqlUpdateVisitor;
    }

    public DmlType getDmlType() {
        return dmlType;
    }

    public UcSqlSelectVisitor getUcSqlSelectVisitor() {
        return ucSqlSelectVisitor;
    }

    public UcSqlDeleteVisitor getUcSqlDeleteVisitor() {
        return ucSqlDeleteVisitor;
    }

    public static void main(String[] args) throws SQLException {
        String sql = "SeLECT ?,b.C,C,b.* FROM l,E_A_* WHERE D.F=23 OR (?=? OR BB>1) AND JJ BETWEEN '200' AND '300' OR PP LIKE '*a*' OR ZZ NOT IN ('a','vv') GROUP BY M,B ORDER BY M,N DESC,L  HIGHLIGHT_COLUMN BY (? as b,Y AS ?) HIGHLIGHT_TYPE IS plain HIGHLIGHT_TAG IS 'tag_start' AND 'tag_end' DEeP_SCAN IS 1 LIMIT ?,10";
        //String sql = "insert into ? (u,?) values ('a','b'),(?,'d')";
        //String sql = "update a set b=1,c=?,?='d',e='f'  WHERE gg=23 OR (?=? OR hh>1) AND JJ BETWEEN '200' AND '300' OR PP LIKE '*a*' OR ZZ NOT IN ('a','vv')";
        //String sql = "update a set b=1,c=?,?='d',e='f'  WHERE (k=3 or (g=6 and h=4 and l=4))";

        UcSqlParser ucSqlParser = new UcSqlParser();
        boolean bParase = ucSqlParser.parserSql(sql);
        if (bParase == false){
            return;
        }
        return;
    }

    public String getErrorMsg() {
        return errorMsg;
    }
}
