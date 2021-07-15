package com.uc.base.parser;

import com.google.common.collect.Lists;
import com.uc.base.antlr.MySqlParser;
import com.uc.base.sql.*;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class UcSqlSelectVisitor extends UcSqlBaseVisitor {
    boolean isCount = false;

    private List<UcSqlVarInfo> listSelectColumnName = Lists.newArrayList();
    private List<UcSqlVarInfo> listSelectTableName = Lists.newArrayList();
    private List<UcSqlLimit> listSelectLimit = Lists.newArrayList();
    private List<UcSqlOrderBy> listSelectOrderBy = Lists.newArrayList();
    private List<UcSqlVarInfo> listSelectGroupBy = Lists.newArrayList();
    private List<UcSqlVarInfo> listSelectHighlightType = Lists.newArrayList();
    private List<UcSqlHighlight> listSelectHighlightColummName = Lists.newArrayList();
    private List<UcSqlVarInfo> listSelectHighlightTag = Lists.newArrayList();
    private List<UcSqlVarInfo> listSelectDeepScan = Lists.newArrayList();

    public List<UcSqlVarInfo> visitTableSources(MySqlParser.TableSourcesContext tableSourcesContext) {
        List<MySqlParser.TableSourceContext> listTableSourceContext = tableSourcesContext.tableSource();
        for(MySqlParser.TableSourceContext tableSourceContext : listTableSourceContext){
            String tableName =tableSourceContext.getText();
            UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(tableName);
            listSelectTableName.add(ucSqlVarInfo);
        }
        return listSelectTableName;
    }

    public List<UcSqlWhere> visitSimpleSelect(MySqlParser.SimpleSelectContext simpleSelectContext) {
        MySqlParser.QuerySpecificationContext querySpecificationContext = simpleSelectContext.querySpecification();
        MySqlParser.SelectElementsContext selectElementsContext = querySpecificationContext.selectElements();
        visitSelectElements(selectElementsContext);


        MySqlParser.FromClauseContext fromClauseContext = querySpecificationContext.fromClause();
        MySqlParser.TableSourcesContext tableSourcesContext = fromClauseContext.tableSources();
        visitTableSources(tableSourcesContext);

        MySqlParser.ExpressionContext expressionContext = fromClauseContext.whereExpr;
        operExpressionContext(expressionContext);

        List<MySqlParser.GroupByItemContext> listGroupByItemContext = fromClauseContext.groupByItem();
        if (CollectionUtils.isEmpty(listGroupByItemContext) == false) {
            for(MySqlParser.GroupByItemContext groupByItemContext : listGroupByItemContext){
                visitGroupByItem(groupByItemContext);
            }
        }

        MySqlParser.OrderByClauseContext orderByClauseContext = querySpecificationContext.orderByClause();
        visitOrderByClause(orderByClauseContext);

        List<MySqlParser.HighlightClauseContext> listHighlightClauseContext = querySpecificationContext.highlightClause();
        if (CollectionUtils.isEmpty(listHighlightClauseContext) == false) {
            for (MySqlParser.HighlightClauseContext highlightClauseContext : listHighlightClauseContext) {
                visitHighlightClause(highlightClauseContext);
            }
        }

        MySqlParser.DeepScanClauseContext deepScanClauseContext = querySpecificationContext.deepScanClause();
        visitDeepScanClause(deepScanClauseContext);

        MySqlParser.LimitClauseContext limitClauseContext = querySpecificationContext.limitClause();
        visitLimitClause(limitClauseContext);

        return listUcSqlWhere;
    }



    public List<UcSqlVarInfo> visitGroupByItem(MySqlParser.GroupByItemContext groupByItemContext) {
        String columName = groupByItemContext.expression().getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(columName);
        listSelectGroupBy.add(ucSqlVarInfo);
        return listSelectGroupBy;
    }

    public List<UcSqlLimit> visitLimitClause(MySqlParser.LimitClauseContext limitClauseContext) {
        if (limitClauseContext == null){
            return listSelectLimit;
        }
        String offset = "0";
        UcSqlVarInfo offsetVar = new UcSqlVarInfo(offset, UcSqlVarInfo.VarType.CONSTANT,0);
        UcSqlVarInfo sizeVar = new UcSqlVarInfo(offset,UcSqlVarInfo.VarType.CONSTANT,0);
        if (limitClauseContext.offset != null) {
            offset = limitClauseContext.offset.getText();
            offsetVar = getUcParserVarInfo(offset);
        }
        String limit = limitClauseContext.limit.getText();
        sizeVar = getUcParserVarInfo(limit);
        String content = offset + ":" + limit;

        UcSqlLimit ucSqlLimit = new UcSqlLimit();
        ucSqlLimit.setOffsetVar(offsetVar);
        ucSqlLimit.setSizeVar(sizeVar);
        listSelectLimit.add(ucSqlLimit);
        return listSelectLimit;
    }

    public List<UcSqlVarInfo> visitDeepScanClause(MySqlParser.DeepScanClauseContext deepScanClauseContext) {
        if (deepScanClauseContext == null){
            return listSelectDeepScan;
        }
        String content = deepScanClauseContext.deepScanFlag.getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(content);
        listSelectDeepScan.add(ucSqlVarInfo);
        return listSelectDeepScan;
    }

    public List<UcSqlHighlight> visitHighlightClause(MySqlParser.HighlightClauseContext highlightClauseContext) {
        MySqlParser.HighlightTypeClauseContext highlightTypeClauseContext = highlightClauseContext.highlightTypeClause();
        visitHighlightTypeClause(highlightTypeClauseContext);
        MySqlParser.HighlightColumnClauseContext highlightColumnClauseContext = highlightClauseContext.highlightColumnClause();
        visitHighlightColumnClause(highlightColumnClauseContext);
        MySqlParser.HighlightTagClauseContext highlightTagClauseContext = highlightClauseContext.highlightTagClause();
        visitHighlightTagClause(highlightTagClauseContext);

        return listSelectHighlightColummName;
    }

    public List<UcSqlHighlight> visitHighlightColumnClause(MySqlParser.HighlightColumnClauseContext highlightColumnClauseContext) {
        if (highlightColumnClauseContext == null){
            return listSelectHighlightColummName;
        }

        List<MySqlParser.HighlightColumnExpressionContext> listHighlightColumnExpressionContext = highlightColumnClauseContext.highlightColumnExpression();
        for(MySqlParser.HighlightColumnExpressionContext highlightColumnExpressionContext : listHighlightColumnExpressionContext) {
            String left = highlightColumnExpressionContext.left.getText();
            UcSqlVarInfo leftUcSqlVarInfo = getUcParserVarInfo(left);
            String right = highlightColumnExpressionContext.right.getText();
            UcSqlVarInfo rightUcSqlVarInfo = getUcParserVarInfo(right);
            UcSqlHighlight ucSqlHighlight = new UcSqlHighlight(leftUcSqlVarInfo,rightUcSqlVarInfo);
            listSelectHighlightColummName.add(ucSqlHighlight);
        }
        return listSelectHighlightColummName;
    }

    public List<UcSqlVarInfo> visitHighlightTagClause(MySqlParser.HighlightTagClauseContext highlightTagClauseContext) {
        if (highlightTagClauseContext == null){
            return listSelectHighlightTag;
        }
        String startTag = highlightTagClauseContext.start.getText();
        UcSqlVarInfo startVarInfo = getUcParserVarInfo(startTag);
        listSelectHighlightTag.add(startVarInfo);
        String endTag = highlightTagClauseContext.end.getText();
        UcSqlVarInfo endVarInfo = getUcParserVarInfo(endTag);
        listSelectHighlightTag.add(endVarInfo);
        return listSelectHighlightTag;
    }

    public List<UcSqlVarInfo> visitHighlightTypeClause(MySqlParser.HighlightTypeClauseContext highlightTypeClauseContext) {
        if (highlightTypeClauseContext == null){
            return listSelectHighlightType;
        }
        String content = "";
        content = highlightTypeClauseContext.type.getText();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(content);
        listSelectHighlightType.add(ucSqlVarInfo);
        return listSelectHighlightType;
    }

    public List<UcSqlOrderBy> visitOrderByClause(MySqlParser.OrderByClauseContext orderByClauseContext) {
        if (orderByClauseContext == null){
            return listSelectOrderBy;
        }
        List<MySqlParser.OrderByExpressionContext> listOrderByExpressionContext = orderByClauseContext.orderByExpression();
        for(MySqlParser.OrderByExpressionContext orderByExpressionContext : listOrderByExpressionContext){
            UcSqlOrderBy ucSqlOrderBy = new UcSqlOrderBy();
            String sort = "ASC";
            UcSqlVarInfo sortVar = new UcSqlVarInfo(sort,UcSqlVarInfo.VarType.CONSTANT,0);

            if (orderByExpressionContext.order != null) {
                sort = orderByExpressionContext.order.getText();
                sortVar = getUcParserVarInfo(sort);
            }
            String columName = orderByExpressionContext.expression().getText();
            UcSqlVarInfo columnVar = getUcParserVarInfo(columName);
            ucSqlOrderBy.setColumnVar(columnVar);
            ucSqlOrderBy.setSortVar(sortVar);
            listSelectOrderBy.add(ucSqlOrderBy);
        }
        return listSelectOrderBy;
    }

    public List<UcSqlVarInfo> visitAggregateFunctionCall(MySqlParser.AggregateFunctionCallContext aggregateFunctionCallContext) {
        MySqlParser.AggregateWindowedFunctionContext aggregateWindowedFunctionContext = aggregateFunctionCallContext.aggregateWindowedFunction();
        TerminalNode terminalNode = aggregateWindowedFunctionContext.COUNT();
        UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(terminalNode.getText());
        listSelectColumnName.add(ucSqlVarInfo);
        isCount = true;
        return listSelectColumnName;
    }

    public List<UcSqlVarInfo> visitSelectElements(MySqlParser.SelectElementsContext selectElementsContext) {
        String columName = "";
        if (selectElementsContext.star != null){
            columName = selectElementsContext.star.getText();
            UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(columName);
            listSelectColumnName.add(ucSqlVarInfo);
        }else{
            List<MySqlParser.SelectElementContext> listSelectElementContext = selectElementsContext.selectElement();
            for(MySqlParser.SelectElementContext selectElementContext : listSelectElementContext){
                if (selectElementContext instanceof MySqlParser.SelectFunctionElementContext) {
                    MySqlParser.SelectFunctionElementContext selectFunctionElementContext = (MySqlParser.SelectFunctionElementContext) selectElementContext;
                    MySqlParser.FunctionCallContext functionCallContext = selectFunctionElementContext.functionCall();
                    if (functionCallContext instanceof MySqlParser.AggregateFunctionCallContext){
                        MySqlParser.AggregateFunctionCallContext aggregateFunctionCallContext = (MySqlParser.AggregateFunctionCallContext) functionCallContext;
                        visitAggregateFunctionCall(aggregateFunctionCallContext);
                    }
                }else{
                    columName = selectElementContext.getText();
                    UcSqlVarInfo ucSqlVarInfo = getUcParserVarInfo(columName);
                    listSelectColumnName.add(ucSqlVarInfo);
                }
            }
        }
        return listSelectColumnName;
    }

    public List<UcSqlVarInfo> getListSelectColumnName() {
        return listSelectColumnName;
    }

    public List<UcSqlVarInfo> getListSelectTableName() {
        return listSelectTableName;
    }

    public List<UcSqlLimit> getListSelectLimit() {
        return listSelectLimit;
    }

    public List<UcSqlOrderBy> getListSelectOrderBy() {
        return listSelectOrderBy;
    }

    public List<UcSqlVarInfo> getListSelectGroupBy() {
        return listSelectGroupBy;
    }


    public List<UcSqlVarInfo> getListSelectHighlightType() {
        return listSelectHighlightType;
    }

    public List<UcSqlHighlight> getListSelectHighlightColummName() {
        return listSelectHighlightColummName;
    }

    public List<UcSqlVarInfo> getListSelectHighlightTag() {
        return listSelectHighlightTag;
    }

    public List<UcSqlVarInfo> getListSelectDeepScan() {
        return listSelectDeepScan;
    }

    public boolean isCount() {
        return isCount;
    }
}
