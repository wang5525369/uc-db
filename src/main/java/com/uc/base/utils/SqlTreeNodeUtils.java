package com.uc.base.utils;

import com.google.common.collect.Lists;
import com.uc.base.enums.UcSqlEnums;
import com.uc.base.parser.SqlTreeNode;
import com.uc.base.sql.UcSqlWhere;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Stack;

public class SqlTreeNodeUtils {
    public static SqlTreeNode generateSqlTreeNode(List<UcSqlWhere> listUpdateWhere){
        Stack<SqlTreeNode> stackLogicSqlTreeNode = new Stack<SqlTreeNode>();
        Stack<SqlTreeNode> stackExpressionSqlTreeNode = new Stack<SqlTreeNode>();


        UcSqlWhere ucSqlWhereNull = new UcSqlWhere(UcSqlEnums.SQL_TOKEN.NULL,null,null);
        SqlTreeNode sqlTreeNode = getSqlTreeNode(ucSqlWhereNull,null);
        stackLogicSqlTreeNode.push(sqlTreeNode);

        int i = 0;
        for(UcSqlWhere ucSqlWhere : listUpdateWhere){
            if (ucSqlWhere.getSql_token() == UcSqlEnums.SQL_TOKEN.AND || ucSqlWhere.getSql_token() == UcSqlEnums.SQL_TOKEN.OR){

                SqlTreeNode frontLogicSqlTreeNode = stackLogicSqlTreeNode.pop();
                if (frontLogicSqlTreeNode.getUcSqlWhere().getSql_token() == UcSqlEnums.SQL_TOKEN.NULL) {
                    SqlTreeNode logicSqlTreeNode = getSqlTreeNode(ucSqlWhere,null);
                    SqlTreeNode expressionSqlTreeNode = stackExpressionSqlTreeNode.pop();
                    setSqlTreeNodeParent(logicSqlTreeNode, expressionSqlTreeNode, true);
                    stackLogicSqlTreeNode.push(logicSqlTreeNode);

                }else{
                    SqlTreeNode logicSqlTreeNode = getSqlTreeNode(ucSqlWhere,null);
                    SqlTreeNode expressionSqlTreeNode = stackExpressionSqlTreeNode.pop();
                    if (frontLogicSqlTreeNode.getUcSqlWhere().getSql_token() == UcSqlEnums.SQL_TOKEN.NULL) {
                        setSqlTreeNodeParent(logicSqlTreeNode, expressionSqlTreeNode, true);
                        if (stackLogicSqlTreeNode.size() > 0){
                            frontLogicSqlTreeNode = stackLogicSqlTreeNode.peek();
                            setSqlTreeNodeParent(frontLogicSqlTreeNode, logicSqlTreeNode, false);
                        }
                    }else {
                        setSqlTreeNodeParent(frontLogicSqlTreeNode, expressionSqlTreeNode, false);
                        setSqlTreeNodeParent(logicSqlTreeNode, frontLogicSqlTreeNode, true);
                    }
                    stackLogicSqlTreeNode.push(logicSqlTreeNode);
                }
            }else if (ucSqlWhere.getSql_token() == UcSqlEnums.SQL_TOKEN.LR){
                ucSqlWhereNull = new UcSqlWhere(UcSqlEnums.SQL_TOKEN.NULL,null,null);
                sqlTreeNode = getSqlTreeNode(ucSqlWhereNull,null);
                stackLogicSqlTreeNode.push(sqlTreeNode);

            }else if (ucSqlWhere.getSql_token() == UcSqlEnums.SQL_TOKEN.RR) {
                SqlTreeNode logicSqlTreeNode = stackLogicSqlTreeNode.pop();
                SqlTreeNode expressionSqlTreeNode = stackExpressionSqlTreeNode.pop();
                if (logicSqlTreeNode.getUcSqlWhere().getSql_token() == UcSqlEnums.SQL_TOKEN.NULL) {
                    UcSqlWhere ucSqlWhereAnd = new UcSqlWhere(UcSqlEnums.SQL_TOKEN.AND,null,null);
                    logicSqlTreeNode = getSqlTreeNode(ucSqlWhereAnd,null);
                }
                setSqlTreeNodeParent(logicSqlTreeNode, expressionSqlTreeNode, false);
                stackExpressionSqlTreeNode.push(logicSqlTreeNode);
            }
            else{
                SqlTreeNode expressionSqlTreeNode = getSqlTreeNode(ucSqlWhere,null);
                stackExpressionSqlTreeNode.push(expressionSqlTreeNode);
            }
            i++;
        }
        SqlTreeNode leaderSqlTreeNode = stackLogicSqlTreeNode.pop();
        if (leaderSqlTreeNode.getUcSqlWhere().getSql_token() == UcSqlEnums.SQL_TOKEN.NULL) {
            UcSqlWhere ucSqlWhereAnd = new UcSqlWhere(UcSqlEnums.SQL_TOKEN.AND,null,null);
            leaderSqlTreeNode = getSqlTreeNode(ucSqlWhereAnd,leaderSqlTreeNode);
        }
        if (stackExpressionSqlTreeNode.size() != 0){
            SqlTreeNode expressionSqlTreeNode = stackExpressionSqlTreeNode.pop();
            setSqlTreeNodeParent(leaderSqlTreeNode,expressionSqlTreeNode,false);
        }
        return leaderSqlTreeNode;
    }

    static public List<List<SqlTreeNode>> getAllRootNode(List<List<SqlTreeNode>> listRootSqlTreeNode){
        int size = listRootSqlTreeNode.size();
        if (size == 0){
            return listRootSqlTreeNode;
        }
        List<SqlTreeNode> listLevelSqlTreeNode = Lists.newArrayList();
        List<SqlTreeNode> listSqlTreeNode = listRootSqlTreeNode.get(size-1);
        for(SqlTreeNode sqlTreeNode : listSqlTreeNode){
            SqlTreeNode leftSqlTreeNode = sqlTreeNode.getLeftChildSqlTreeNode();
            SqlTreeNode rightSqlTreeNode = sqlTreeNode.getRightChildSqlTreeNode();
            listLevelSqlTreeNode = addLevelSqlTreeNode(leftSqlTreeNode,listLevelSqlTreeNode);
            listLevelSqlTreeNode = addLevelSqlTreeNode(rightSqlTreeNode,listLevelSqlTreeNode);
        }
        if (CollectionUtils.isEmpty(listLevelSqlTreeNode) == false){
            listRootSqlTreeNode.add(listLevelSqlTreeNode);
            listRootSqlTreeNode = getAllRootNode(listRootSqlTreeNode);
        }
        return listRootSqlTreeNode;
    }

    static public List<SqlTreeNode> addLevelSqlTreeNode(SqlTreeNode sqlTreeNode,List<SqlTreeNode> listLevelSqlTreeNode){
        if (sqlTreeNode != null) {
            boolean isLastSqlTreeNode = isLastSqlTreeNode(sqlTreeNode);
            if (isLastSqlTreeNode == false) {
                listLevelSqlTreeNode.add(sqlTreeNode);
            }
        }
        return listLevelSqlTreeNode;
    }

    static public boolean isLastSqlTreeNode(SqlTreeNode sqlTreeNode){
        boolean bRet = false;
        if (sqlTreeNode.getLeftChildSqlTreeNode() != null || sqlTreeNode.getRightChildSqlTreeNode() != null){
            return bRet;
        }
        return bRet = true;
    }

    static private  SqlTreeNode getSqlTreeNode(UcSqlWhere ucSqlWhere, SqlTreeNode copySqlTreeNode){
        SqlTreeNode sqlTreeNode = new SqlTreeNode();
        sqlTreeNode.setUcSqlWhere(ucSqlWhere);
        if (copySqlTreeNode != null){
            sqlTreeNode.setRightChildSqlTreeNode(copySqlTreeNode.getRightChildSqlTreeNode());
            sqlTreeNode.setLeftChildSqlTreeNode(copySqlTreeNode.getLeftChildSqlTreeNode());
        }
        return sqlTreeNode;
    }

    static private void setSqlTreeNodeParent(SqlTreeNode parentSqlTreeNode,SqlTreeNode childSqlTreeNode,boolean isLeftSqlTreeNode){
        if (isLeftSqlTreeNode == true){
            parentSqlTreeNode.setLeftChildSqlTreeNode(childSqlTreeNode);
        }else{
            parentSqlTreeNode.setRightChildSqlTreeNode(childSqlTreeNode);
        }
    }


    // 用于获得树的层数
    public static int getTreeDepth(SqlTreeNode root) {
        return root == null ? 0 : (1 + Math.max(getTreeDepth(root.getLeftChildSqlTreeNode()), getTreeDepth(root.getRightChildSqlTreeNode())));
    }


    private static void writeArray(SqlTreeNode currNode, int rowIndex, int columnIndex, String[][] res, int treeDepth) {
        // 保证输入的树不为空
        if (currNode == null) return;
        // 先将当前节点保存到二维数组中
        String show = currNode.getUcSqlWhere().getSql_token().getName();
        if (currNode.getUcSqlWhere().getLetfUcSqlVarInfo() != null) {
            show = currNode.getUcSqlWhere().getLetfUcSqlVarInfo().getVarValue() + currNode.getUcSqlWhere().getSql_token().getName();
        }
        res[rowIndex][columnIndex] = String.valueOf(show);

        // 计算当前位于树的第几层
        int currLevel = ((rowIndex + 1) / 2);
        // 若到了最后一层，则返回
        if (currLevel == treeDepth) return;
        // 计算当前行到下一行，每个元素之间的间隔（下一行的列索引与当前元素的列索引之间的间隔）
        int gap = treeDepth - currLevel - 1;

        // 对左儿子进行判断，若有左儿子，则记录相应的"/"与左儿子的值
        if (currNode.getLeftChildSqlTreeNode() != null) {
            res[rowIndex + 1][columnIndex - gap] = "/ ";
            writeArray(currNode.getLeftChildSqlTreeNode(), rowIndex + 2, columnIndex - gap * 2, res, treeDepth);
        }

        // 对右儿子进行判断，若有右儿子，则记录相应的"\"与右儿子的值
        if (currNode.getRightChildSqlTreeNode() != null) {
            res[rowIndex + 1][columnIndex + gap] = "\\ ";
            writeArray(currNode.getRightChildSqlTreeNode(), rowIndex + 2, columnIndex + gap * 2, res, treeDepth);
        }
    }


    public static void show(SqlTreeNode root) {
        if (root == null) System.out.println("EMPTY!");
        // 得到树的深度
        int treeDepth = getTreeDepth(root);

        // 最后一行的宽度为2的（n - 1）次方乘3，再加1
        // 作为整个二维数组的宽度
        int arrayHeight = treeDepth * 2 - 1;
        int arrayWidth = (2 << (treeDepth - 2)) * 3 + 1;
        // 用一个字符串数组来存储每个位置应显示的元素
        String[][] res = new String[arrayHeight][arrayWidth];
        // 对数组进行初始化，默认为一个空格
        for (int i = 0; i < arrayHeight; i ++) {
            for (int j = 0; j < arrayWidth; j ++) {
                res[i][j] = " ";
            }
        }

        // 从根节点开始，递归处理整个树
        // res[0][(arrayWidth + 1)/ 2] = (char)(root.val + '0');
        writeArray(root, 0, arrayWidth/2, res, treeDepth);

        // 此时，已经将所有需要显示的元素储存到了二维数组中，将其拼接并打印即可
        for (String[] line: res) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < line.length; i ++) {
                sb.append(line[i]);
                if (line[i].length() > 1 && i <= line.length - 1) {
                    i += line[i].length() > 4 ? 2: line[i].length() - 1;
                }
            }
            System.out.println(sb.toString());
        }
    }
}
