package com.uc.base.parser;

import com.google.common.collect.Lists;
import com.uc.base.enums.UcSqlEnums;
import com.uc.base.utils.SqlTreeNodeUtils;
import com.uc.base.antlr.MySqlParser;
import com.uc.base.antlr.MySqlParserBaseVisitor;
import com.uc.base.sql.UcSqlVarInfo;
import com.uc.base.sql.UcSqlWhere;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class UcSqlBaseVisitor extends MySqlParserBaseVisitor {
    private int varIndex = 1;

    public SqlTreeNode whereSqlTreeNode = null;
    public List<List<SqlTreeNode>> listRootWhereSqlTreeNode = Lists.newArrayList();
    public List<UcSqlWhere> listUcSqlWhere = Lists.newArrayList();

    public void operExpressionContext(MySqlParser.ExpressionContext expressionContext ){
        parserExpressionContext(expressionContext);
        whereSqlTreeNode = SqlTreeNodeUtils.generateSqlTreeNode(listUcSqlWhere);
        //SqlTreeNodeUtils.show(whereSqlTreeNode);
        listRootWhereSqlTreeNode = getRootWhereSqlTreeNode();
    }

    public void parserExpressionContext(MySqlParser.ExpressionContext expressionContext ){
        if (expressionContext instanceof MySqlParser.LogicalExpressionContext){
            MySqlParser.LogicalExpressionContext logicalExpressionContext = (MySqlParser.LogicalExpressionContext) expressionContext;
            visitLogicalExpression(logicalExpressionContext);
        }else if (expressionContext instanceof  MySqlParser.PredicateExpressionContext){
            MySqlParser.PredicateExpressionContext  predicateExpressionContext = (MySqlParser.PredicateExpressionContext) expressionContext;
            visitPredicateExpression(predicateExpressionContext);
        }
    }

    public List<UcSqlWhere> visitLogicalExpression(MySqlParser.LogicalExpressionContext logicalExpressionContext){
        List<MySqlParser.ExpressionContext> listExpressionContext = logicalExpressionContext.expression();
        if (CollectionUtils.isEmpty(listExpressionContext) == true){
            return listUcSqlWhere;
        }
        int i = 0;
        parserExpressionContext(listExpressionContext.get(i));
        MySqlParser.LogicalOperatorContext logicalOperatorContext = logicalExpressionContext.logicalOperator();
        visitLogicalOperator(logicalOperatorContext);
        i++;
        if (i >= listExpressionContext.size()){
            return listUcSqlWhere;
        }
        parserExpressionContext(listExpressionContext.get(i));
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> visitLogicalOperator(MySqlParser.LogicalOperatorContext logicalOperatorContext) {
        String oper = logicalOperatorContext.getText();
        UcSqlEnums.SQL_TOKEN sql_token = UcSqlEnums.SQL_TOKEN.getByName(oper);
        UcSqlWhere ucSqlWhere = new UcSqlWhere(sql_token,null,null);
        listUcSqlWhere.add(ucSqlWhere);
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> visitPredicateExpression(MySqlParser.PredicateExpressionContext predicateExpressionContext){
        MySqlParser.PredicateContext predicateContext = predicateExpressionContext.predicate();
        if (predicateContext instanceof MySqlParser.BracketPredicateContext){
            MySqlParser.BracketPredicateContext bracketPredicateContext = (MySqlParser.BracketPredicateContext) predicateContext;
            visitBracketPredicate(bracketPredicateContext);
        }else if (predicateContext instanceof MySqlParser.InPredicateContext){
            MySqlParser.InPredicateContext inPredicateContext = (MySqlParser.InPredicateContext) predicateContext;
            enterInPredicate(inPredicateContext);
        }if (predicateContext instanceof MySqlParser.LikePredicateContext){
            MySqlParser.LikePredicateContext likePredicateContext = (MySqlParser.LikePredicateContext) predicateContext;
            enterLikePredicate(likePredicateContext);
        }if (predicateContext instanceof MySqlParser.BetweenPredicateContext){
            MySqlParser.BetweenPredicateContext betweenPredicateContext = (MySqlParser.BetweenPredicateContext) predicateContext;
            enterBetweenPredicate(betweenPredicateContext);
        }if (predicateContext instanceof MySqlParser.BinaryComparasionPredicateContext){
            MySqlParser.BinaryComparasionPredicateContext binaryComparasionPredicateContext = (MySqlParser.BinaryComparasionPredicateContext) predicateContext;
            enterBinaryComparasionPredicate(binaryComparasionPredicateContext);
        }
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> enterBinaryComparasionPredicate(MySqlParser.BinaryComparasionPredicateContext binaryComparasionPredicateContext) {
        String left = binaryComparasionPredicateContext.left.getText();
        UcSqlVarInfo leftUcSqlVarInfo = getUcParserVarInfo(left);
        String oper = binaryComparasionPredicateContext.comparisonOperator().getText();
        UcSqlEnums.SQL_TOKEN sql_token = UcSqlEnums.SQL_TOKEN.getByName(oper);
        String right = binaryComparasionPredicateContext.right.getText();
        UcSqlVarInfo rightUcSqlVarInfo = getUcParserVarInfo(right);
        List<UcSqlVarInfo> listRightUcSqlVarInfo = Lists.newArrayList();
        listRightUcSqlVarInfo.add(rightUcSqlVarInfo);
        UcSqlWhere ucSqlWhere = new UcSqlWhere(sql_token,leftUcSqlVarInfo,listRightUcSqlVarInfo);
        listUcSqlWhere.add(ucSqlWhere);
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> enterBetweenPredicate(MySqlParser.BetweenPredicateContext betweenPredicateContext) {
        UcSqlEnums.SQL_TOKEN sql_token = UcSqlEnums.SQL_TOKEN.BETWEEN;
        if (betweenPredicateContext.NOT() != null){
            sql_token = UcSqlEnums.SQL_TOKEN.NOT_BETWEEN;
        }

        List<MySqlParser.PredicateContext> listPredicateContext = betweenPredicateContext.predicate();
        String columName = listPredicateContext.get(0).getText();
        UcSqlVarInfo leftUcSqlVarInfo = getUcParserVarInfo(columName);

        List<UcSqlVarInfo> listRightUcSqlVarInfo = Lists.newArrayList();

        String startContent = listPredicateContext.get(1).getText();
        UcSqlVarInfo startUcSqlVarInfo = getUcParserVarInfo(startContent);
        listRightUcSqlVarInfo.add(startUcSqlVarInfo);

        String endContent = listPredicateContext.get(2).getText();
        UcSqlVarInfo endUcSqlVarInfo = getUcParserVarInfo(endContent);
        listRightUcSqlVarInfo.add(endUcSqlVarInfo);

        UcSqlWhere ucSqlWhere = new UcSqlWhere(sql_token,leftUcSqlVarInfo,listRightUcSqlVarInfo);
        listUcSqlWhere.add(ucSqlWhere);
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> enterLikePredicate(MySqlParser.LikePredicateContext likePredicateContext) {
        UcSqlEnums.SQL_TOKEN sql_token = UcSqlEnums.SQL_TOKEN.LIKE;
        if (likePredicateContext.NOT() != null){
            sql_token = UcSqlEnums.SQL_TOKEN.NOT_LIKE;
        }

        List<MySqlParser.PredicateContext> listPredicateContext = likePredicateContext.predicate();

        String columName = listPredicateContext.get(0).getText();
        UcSqlVarInfo leftUcSqlVarInfo = getUcParserVarInfo(columName);

        List<UcSqlVarInfo> listRightUcSqlVarInfo = Lists.newArrayList();

        String content = listPredicateContext.get(1).getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(content);
        listRightUcSqlVarInfo.add(ucSqlVarInfo);

        UcSqlWhere ucSqlWhere = new UcSqlWhere(sql_token,leftUcSqlVarInfo,listRightUcSqlVarInfo);
        listUcSqlWhere.add(ucSqlWhere);
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> enterInPredicate(MySqlParser.InPredicateContext inPredicateContext) {
        UcSqlEnums.SQL_TOKEN sql_token = UcSqlEnums.SQL_TOKEN.IN;
        if (inPredicateContext.NOT() != null){
            sql_token = UcSqlEnums.SQL_TOKEN.NOT_IN;
        }

        MySqlParser.PredicateContext predicateContext = inPredicateContext.predicate();
        String columName = predicateContext.getText();
        UcSqlVarInfo leftUcSqlVarInfo = getUcParserVarInfo(columName);

        List<UcSqlVarInfo> listRightUcSqlVarInfo = Lists.newArrayList();
        MySqlParser.ExpressionsContext expressionsContext = inPredicateContext.expressions();
        List<MySqlParser.ExpressionContext> listExpressionContext = expressionsContext.expression();
        for(MySqlParser.ExpressionContext temp : listExpressionContext){
            String content = temp.getText();
            UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(content);
            listRightUcSqlVarInfo.add(ucSqlVarInfo);
        }
        UcSqlWhere ucSqlWhere = new UcSqlWhere(sql_token,leftUcSqlVarInfo,listRightUcSqlVarInfo);
        listUcSqlWhere.add(ucSqlWhere);
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> visitBracketPredicate(MySqlParser.BracketPredicateContext bracketPredicateContext){
        enterBracketPredicate(bracketPredicateContext);
        parserExpressionContext(bracketPredicateContext.expression());
        exitBracketPredicate(bracketPredicateContext);
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> enterBracketPredicate(MySqlParser.BracketPredicateContext bracketPredicateContext) {
        UcSqlEnums.SQL_TOKEN sql_token = UcSqlEnums.SQL_TOKEN.LR;
        UcSqlWhere ucSqlWhere = new UcSqlWhere(sql_token,null,null);
        listUcSqlWhere.add(ucSqlWhere);
        return listUcSqlWhere;
    }

    public List<UcSqlWhere> exitBracketPredicate(MySqlParser.BracketPredicateContext bracketPredicateContext) {
        UcSqlEnums.SQL_TOKEN sql_token = UcSqlEnums.SQL_TOKEN.RR;
        UcSqlWhere ucSqlWhere = new UcSqlWhere(sql_token,null,null);
        listUcSqlWhere.add(ucSqlWhere);
        return listUcSqlWhere;
    }

    public UcSqlVarInfo getUcParserVarInfo(String value){
        UcSqlVarInfo.VarType varType = UcSqlVarInfo.VarType.CONSTANT;
        int index = -1;
        String content = value;
        if (StringUtils.isEmpty(value) == false){
            if ("?".equals(value) == true){
                varType = UcSqlVarInfo.VarType.VARIABLE;
                index = varIndex++;
            }else if ( value.startsWith("'") == true && value.endsWith("'") == true){
                content = content.substring(1,content.length()-1);
            }else if ( value.startsWith("`") == true && value.endsWith("`") == true){
                content = content.substring(1,content.length()-1);
            }
        }
        UcSqlVarInfo ucSqlVarInfo = new UcSqlVarInfo(content,varType,index);
        return ucSqlVarInfo;
    }

    List<List<SqlTreeNode>> getRootWhereSqlTreeNode(){
        if (whereSqlTreeNode.getUcSqlWhere().getSql_token() == UcSqlEnums.SQL_TOKEN.AND || whereSqlTreeNode.getUcSqlWhere().getSql_token() == UcSqlEnums.SQL_TOKEN.OR){
            if (whereSqlTreeNode.getLeftChildSqlTreeNode() != null || whereSqlTreeNode.getRightChildSqlTreeNode() != null){
                listRootWhereSqlTreeNode.add(Lists.newArrayList(whereSqlTreeNode));
                listRootWhereSqlTreeNode = SqlTreeNodeUtils.getAllRootNode(listRootWhereSqlTreeNode);
            }
        }
        return listRootWhereSqlTreeNode;
    }

    public SqlTreeNode getWhereSqlTreeNode() {
        return whereSqlTreeNode;
    }

    public List<List<SqlTreeNode>> getListRootWhereSqlTreeNode() {
        return listRootWhereSqlTreeNode;
    }

    public List<UcSqlWhere> getListUcSqlWhere() {
        return listUcSqlWhere;
    }
}
