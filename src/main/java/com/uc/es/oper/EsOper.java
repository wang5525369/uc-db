package com.uc.es.oper;

import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uc.base.jdbc.UcTableInfo;
import com.uc.base.oper.DbOper;
import com.uc.base.parser.UcSqlParser;
import com.uc.base.sql.*;
import com.uc.base.enums.UcSqlEnums;
import com.uc.base.jdbc.UcColumnInfo;
import com.uc.base.jdbc.UcParamInfo;
import com.uc.base.parser.SqlTreeNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class EsOper implements DbOper {

    private static final Logger logger = LoggerFactory.getLogger(EsOper.class);
    private boolean closed = true;
    private TransportClient transportClient = null;
    private String prefixJdbc = "es:mysql://";
    private String esPrefix = "es://";
    private static String retDmlColumnName = "count(0)";
    private boolean bDebug = false;

    public List<Object> queryInfo(TransportClient transportClient, UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<Object> listObject = Lists.newArrayList();
        String tableName = getTableName(ucSqlParser.getUcSqlSelectVisitor().getListSelectTableName(),mapParam);
        Pair<String,String> pairTableInfo = getTableAndTypeName(tableName);
        tableName = pairTableInfo.getLeft();
        String typeName = pairTableInfo.getRight();
        SearchRequestBuilder searchRequestBuilder = getSearchRequestBuilder(transportClient,tableName,typeName);

        if (ucSqlParser.getUcSqlSelectVisitor().isCount() == true) {
            Map<String,String> mapHighlightInfo = getHighlightInfo(ucSqlParser,mapParam);
            addWhere(searchRequestBuilder, ucSqlParser.getUcSqlSelectVisitor().getListRootWhereSqlTreeNode(),mapParam,mapHighlightInfo);
            UcSqlLimit ucSqlLimit = new UcSqlLimit();
            UcSqlVarInfo offsetVar = new UcSqlVarInfo("0", UcSqlVarInfo.VarType.CONSTANT,-1);
            ucSqlLimit.setOffsetVar(offsetVar);
            UcSqlVarInfo sizeVar = new UcSqlVarInfo("1", UcSqlVarInfo.VarType.CONSTANT,-1);
            ucSqlLimit.setSizeVar(sizeVar);
            addFormSize(searchRequestBuilder, Lists.newArrayList(ucSqlLimit),mapParam);
            showDebugInfo("请求:" + searchRequestBuilder.toString());
            SearchResponse response = searchRequestBuilder.execute().actionGet();
            showDebugInfo("返回:" + response.toString());
            UcSqlVarInfo ucSqlVarInfo = ucSqlParser.getUcSqlSelectVisitor().getListSelectColumnName().get(0);
            String columnName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(columnName,response.getHits().totalHits);
            listObject.add(jsonObject);
        }else{
            Map<String,String> mapHighlightInfo = getHighlightInfo(ucSqlParser,mapParam);
            addField(searchRequestBuilder, ucSqlParser.getUcSqlSelectVisitor().getListSelectColumnName(),mapParam);
            addWhere(searchRequestBuilder, ucSqlParser.getUcSqlSelectVisitor().getListRootWhereSqlTreeNode(),mapParam,mapHighlightInfo);
            addSortBuilder(searchRequestBuilder, ucSqlParser.getUcSqlSelectVisitor().getListSelectOrderBy(),mapParam);
            addHighlight(searchRequestBuilder, ucSqlParser.getUcSqlSelectVisitor().getListSelectHighlightType(), ucSqlParser.getUcSqlSelectVisitor().getListSelectHighlightTag(), ucSqlParser.getUcSqlSelectVisitor().getListSelectHighlightColummName(),mapParam);
            addFormSize(searchRequestBuilder, ucSqlParser.getUcSqlSelectVisitor().getListSelectLimit(),mapParam);
            showDebugInfo("请求:" + searchRequestBuilder.toString());
            SearchResponse response = searchRequestBuilder.execute().actionGet();
            showDebugInfo("返回:" + response.toString());


            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String,Object> mapObject = hit.getSourceAsMap();
                JSONObject jsonObject = JSONObject.parseObject(hit.getSourceAsString());
                for(String columnHighlightColummName : mapHighlightInfo.keySet()) {
                    String columnHighlightAliasName = mapHighlightInfo.get(columnHighlightColummName);
                    String highlightContent = hit.getHighlightFields().get(columnHighlightColummName).fragments()[0].toString();
                    jsonObject.put(columnHighlightAliasName,highlightContent);
                }
                listObject.add(jsonObject);
            }
        }
        showDebugInfo("返回记录集:" + JSONObject.toJSONString(listObject));
        return listObject;
    }

    Map<String,String> getHighlightInfo(UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam){
        List<UcSqlHighlight> listUcSqlHighlight = ucSqlParser.getUcSqlSelectVisitor().getListSelectHighlightColummName();
        Map<String,String> mapHighlightInfo = Maps.newHashMap();
        for(UcSqlHighlight ucSqlHighlight : listUcSqlHighlight){
            UcSqlVarInfo leftUcSqlVarInfo = ucSqlHighlight.getLetfUcSqlVarInfo();
            String columnHighlightColummName = getSqlVarValue(leftUcSqlVarInfo,mapParam).toString();
            UcSqlVarInfo rightUcSqlVarInfo = ucSqlHighlight.getRightUcSqlVarInfo();
            String columnHighlightAliasName = getSqlVarValue(rightUcSqlVarInfo,mapParam).toString();
            mapHighlightInfo.put(columnHighlightColummName,columnHighlightAliasName);
        }
        return mapHighlightInfo;
    }

    private void showDebugInfo(String message){
        if (bDebug == true){
            logger.info(message);
        }
    }

    SearchRequestBuilder getSearchRequestBuilder(TransportClient transportClient,String tableName,String typeNme) throws SQLException {
        SearchRequestBuilder searchRequestBuilder = null;
        if (StringUtils.isEmpty(typeNme) == false) {
            searchRequestBuilder = transportClient.prepareSearch(tableName).setTypes(typeNme);
        }else{
            searchRequestBuilder = transportClient.prepareSearch(tableName);
        }
        return searchRequestBuilder;
    }

    void addSortBuilder(SearchRequestBuilder searchRequestBuilder, List<UcSqlOrderBy> listOrderBy, Map<Integer, UcParamInfo> mapParam){
        List<SortBuilder> listSortBuilder = Lists.newArrayList();
        for(UcSqlOrderBy ucSqlOrderBy : listOrderBy){
            UcSqlVarInfo columnVar = ucSqlOrderBy.getColumnVar();
            String columnName = getSqlVarValue(columnVar,mapParam).toString();
            SortBuilder sortBuilder = SortBuilders.fieldSort(columnName);
            UcSqlVarInfo sortVar = ucSqlOrderBy.getSortVar();
            String sort = getSqlVarValue(sortVar,mapParam).toString();
            sort = sort.toUpperCase();
            if ("ASC".equals(sort) == true){
                sortBuilder.order(SortOrder.ASC);
            }else {
                sortBuilder.order(SortOrder.DESC);
            }
            searchRequestBuilder.addSort(sortBuilder);
        }
    }

    void addFormSize(SearchRequestBuilder searchRequestBuilder, List<UcSqlLimit> listLimit, Map<Integer, UcParamInfo> mapParam){
        if (listLimit.size() == 0){
            return;
        }
        UcSqlLimit ucSqlLimit = listLimit.get(0);
        UcSqlVarInfo offsetVar = ucSqlLimit.getOffsetVar();
        UcSqlVarInfo sizeVar = ucSqlLimit.getSizeVar();
        int offset = Integer.parseInt(getSqlVarValue(offsetVar,mapParam).toString());
        int size = Integer.parseInt(getSqlVarValue(sizeVar,mapParam).toString());
        searchRequestBuilder.setFrom(offset).setSize(size);
    }

    String getTableName(List<UcSqlVarInfo> listTableName, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        if (listTableName.size() == 0){
            throw new SQLException("查询表名不能为空 ");
        }
        String tableName = "";
        for(UcSqlVarInfo ucSqlVarInfo : listTableName){
            tableName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
        }
        if (tableName.indexOf("$") >=0){
            tableName = tableName.replace("$","*");
        }
        return tableName;
    }

    void addField(SearchRequestBuilder searchRequestBuilder, List<UcSqlVarInfo> listSelectColumName, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        if (listSelectColumName.size() == 0){
            throw new SQLException("查询列名不能为空 ");
        }

        List<String> listColumnName = Lists.newArrayList();
        for(UcSqlVarInfo ucSqlVarInfo : listSelectColumName){
            String columnName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
            if ("*".equals(columnName) == true){
                break;
            }
            columnName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
            listColumnName.add(columnName);
        }

        if (listColumnName.isEmpty() == false){
            String [] arrayColumnName = listColumnName.toArray(new String[listColumnName.size()]);
            searchRequestBuilder.setFetchSource(arrayColumnName,null);
        }
    }

    QueryBuilder addWhere(SearchRequestBuilder searchRequestBuilder, List<List<SqlTreeNode>> listRootWhereSqlTreeNode, Map<Integer, UcParamInfo> mapParam,Map<String,String> mapHighlightInfo) throws SQLException {
        if (CollectionUtils.isEmpty(listRootWhereSqlTreeNode) == true){
            return null;
        }
        QueryBuilder rootQueryBuilder = null;
        Map<SqlTreeNode,QueryBuilder> mapQueryBuilder = Maps.newHashMap();
        int size = listRootWhereSqlTreeNode.size();
        for(int i = size-1; i>=0; i--){
            List<SqlTreeNode> listSqlTreeNode = listRootWhereSqlTreeNode.get(i);
            for(SqlTreeNode sqlTreeNode : listSqlTreeNode){
                SqlTreeNode leftSqlTreeNode = sqlTreeNode.getLeftChildSqlTreeNode();
                SqlTreeNode rightSqlTreeNode = sqlTreeNode.getRightChildSqlTreeNode();
                List<QueryBuilder> listQueryBuilder = Lists.newArrayList();
                if (leftSqlTreeNode != null){
                    QueryBuilder leftQueryBuilder = getQueryBuilder(leftSqlTreeNode,mapParam,mapQueryBuilder,mapHighlightInfo);
                    listQueryBuilder.add(leftQueryBuilder);
                }
                if (rightSqlTreeNode != null){
                    QueryBuilder rightQueryBuilder = getQueryBuilder(rightSqlTreeNode,mapParam,mapQueryBuilder,mapHighlightInfo);
                    listQueryBuilder.add(rightQueryBuilder);
                }
                rootQueryBuilder = getQueryBuilder(sqlTreeNode,listQueryBuilder,mapQueryBuilder);
            }
        }
        searchRequestBuilder.setQuery(rootQueryBuilder);
        return rootQueryBuilder;
    }

    QueryBuilder getQueryBuilder(SqlTreeNode sqlTreeNode,List<QueryBuilder> listChileQueryBuilder,Map<SqlTreeNode,QueryBuilder> mapQueryBuilder){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        QueryBuilder queryBuilder = mapQueryBuilder.get(sqlTreeNode);
        if (queryBuilder == null){
            UcSqlWhere ucSqlWhere = sqlTreeNode.getUcSqlWhere();
            UcSqlEnums.SQL_TOKEN sql_token = ucSqlWhere.getSql_token();
            List<QueryBuilder> listQueryBuilder = Lists.newArrayList();
            if (sql_token == UcSqlEnums.SQL_TOKEN.AND){
                listQueryBuilder = boolQueryBuilder.must();
            }else if (sql_token == UcSqlEnums.SQL_TOKEN.OR){
                listQueryBuilder = boolQueryBuilder.should();
            }
            listQueryBuilder = addQueryBuilder(listQueryBuilder,listChileQueryBuilder);
            mapQueryBuilder.put(sqlTreeNode,boolQueryBuilder);
        }
        return boolQueryBuilder;
    }

    List<QueryBuilder> addQueryBuilder(List<QueryBuilder> listQueryBuilder,List<QueryBuilder> listChileQueryBuilder){
        for(QueryBuilder queryBuilder : listChileQueryBuilder){
            listQueryBuilder.add(queryBuilder);
        }
        return listQueryBuilder;
    }

    QueryBuilder getQueryBuilder(SqlTreeNode sqlTreeNode,Map<Integer, UcParamInfo> mapParam,Map<SqlTreeNode,QueryBuilder> mapQueryBuilder,Map<String,String> mapHighlightInfo) throws SQLException {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        QueryBuilder queryBuilder = mapQueryBuilder.get(sqlTreeNode);
        if (queryBuilder == null){
            UcSqlWhere ucSqlWhere = sqlTreeNode.getUcSqlWhere();
            UcSqlEnums.SQL_TOKEN sql_token = ucSqlWhere.getSql_token();
            List<QueryBuilder> listQueryBuilder = Lists.newArrayList();
            queryBuilder = QueryBuilders.boolQuery();
            if (sql_token == UcSqlEnums.SQL_TOKEN.AND){
                listQueryBuilder = boolQueryBuilder.must();
                queryBuilder = boolQueryBuilder;
            }else if (sql_token == UcSqlEnums.SQL_TOKEN.OR){
                listQueryBuilder = boolQueryBuilder.should();
                queryBuilder = boolQueryBuilder;
            } else {
                queryBuilder = getQuery(ucSqlWhere, mapParam,mapHighlightInfo);

            }
            mapQueryBuilder.put(sqlTreeNode,queryBuilder);
        }
        return queryBuilder;
    }

    QueryBuilder getQuery(UcSqlWhere ucSqlWhere,Map<Integer, UcParamInfo> mapParam,Map<String,String> mapHighlightInfo) throws SQLException {
        UcSqlEnums.SQL_TOKEN sql_token = ucSqlWhere.getSql_token();
        UcSqlVarInfo leftUcSqlVarInfo = ucSqlWhere.getLetfUcSqlVarInfo();
        List<UcSqlVarInfo> listUcSqlVarInfo = ucSqlWhere.getListRightUcSqlVarInfo();
        String columnName = getSqlVarValue(leftUcSqlVarInfo,mapParam).toString();
        QueryBuilder queryBuilder = null;
        switch (sql_token){
            case EQ:
                {
                    UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                    Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                    if (mapHighlightInfo.containsKey(columnName) == true){
                        queryBuilder = QueryBuilders.multiMatchQuery(columnValue,columnName);
                    }else
                    {
                        queryBuilder = QueryBuilders.matchPhraseQuery(columnName, columnValue);
                    }
                }
                break;
            case NE:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                queryBuilder = boolQueryBuilder.mustNot(QueryBuilders.matchPhraseQuery(columnName, columnValue));
            }
            break;
            case GT:
                {
                    UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                    Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                    queryBuilder = QueryBuilders.rangeQuery(columnName).gt(columnValue);
                }
                break;
            case GTE:
                {
                    UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                    Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                    queryBuilder = QueryBuilders.rangeQuery(columnName).gte(columnValue);
                }
                break;
            case LT:
                {
                    UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                    Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                    queryBuilder = QueryBuilders.rangeQuery(columnName).lt(columnValue);
                }
                break;
            case LTE:
                {
                    UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                    Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                    queryBuilder = QueryBuilders.rangeQuery(columnName).lte(columnValue);
                }
                break;
            case LIKE:
                {
                    UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                    String columnValue = getSqlVarValue(ucSqlVarInfo, mapParam).toString();
                    queryBuilder = QueryBuilders.wildcardQuery(columnName, columnValue);

                }
                break;
            case NOT_LIKE:
                {
                    UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                    String columnValue = getSqlVarValue(ucSqlVarInfo, mapParam).toString();
                    WildcardQueryBuilder wildcardQuery  = QueryBuilders.wildcardQuery(columnName, columnValue);
                    BoolQueryBuilder tempBoolQueryBuilder  = QueryBuilders.boolQuery();
                    tempBoolQueryBuilder.mustNot(wildcardQuery);
                    queryBuilder = tempBoolQueryBuilder;
                }
                break;
            case IN:
                {
                    BoolQueryBuilder tempBoolQueryBuilder = QueryBuilders.boolQuery();
                    for(UcSqlVarInfo ucSqlVarInfo : listUcSqlVarInfo) {
                        Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery(columnName, columnValue);
                        tempBoolQueryBuilder.should(matchPhraseQueryBuilder);
                    }
                    queryBuilder = tempBoolQueryBuilder;
                }
                break;
            case NOT_IN:
                {
                    BoolQueryBuilder tempBoolQueryBuilder = QueryBuilders.boolQuery();
                    for(UcSqlVarInfo ucSqlVarInfo : listUcSqlVarInfo) {
                        Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery(columnName, columnValue);
                        tempBoolQueryBuilder.mustNot(matchPhraseQueryBuilder);
                    }
                    queryBuilder = tempBoolQueryBuilder;
                }
                break;
            case BETWEEN:
                {
                    UcSqlVarInfo startSqlVarInfo = listUcSqlVarInfo.get(0);
                    UcSqlVarInfo endSqlVarInfo = listUcSqlVarInfo.get(1);
                    Object startValue = getSqlVarValue(startSqlVarInfo, mapParam);
                    Object endValue = getSqlVarValue(endSqlVarInfo, mapParam);
                    queryBuilder = QueryBuilders.rangeQuery(columnName).gte(startValue).lte(endValue);
                }
                break;
            default:
            {
                throw new SQLException("不支持的查询"+ sql_token.getName());
            }
        }
        return queryBuilder;
    }

    void addHighlight(SearchRequestBuilder searchRequestBuilder, List<UcSqlVarInfo> listHighlightType, List<UcSqlVarInfo> listHighlightTag, List<UcSqlHighlight> listHighlightColummName, Map<Integer, UcParamInfo> mapParam){
        if (CollectionUtils.isEmpty(listHighlightColummName) == true){
            return;
        }
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        if (listHighlightType.size() > 0){
            UcSqlVarInfo ucSqlVarInfo = listHighlightType.get(0);
            String highlightType = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
            highlightBuilder.highlighterType(highlightType);
        }
        if (listHighlightTag.size() == 2){
            UcSqlVarInfo preUcSqlVarInfo = listHighlightType.get(0);
            String preTags = getSqlVarValue(preUcSqlVarInfo,mapParam).toString();
            UcSqlVarInfo postUcSqlVarInfo = listHighlightType.get(1);
            String postTags = getSqlVarValue(postUcSqlVarInfo,mapParam).toString();
            highlightBuilder.preTags(preTags);
            highlightBuilder.postTags(postTags);
        }

        for (UcSqlHighlight ucSqlHighlight : listHighlightColummName){
            UcSqlVarInfo ucSqlVarInfo = ucSqlHighlight.getLetfUcSqlVarInfo();
            String columnName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
            highlightBuilder.field(columnName);
            highlightBuilder.requireFieldMatch(false);
        }
        highlightBuilder.numOfFragments(0);

        searchRequestBuilder.highlighter(highlightBuilder);
    }

    public List<UcTableInfo> getAllIndex(TransportClient transportClient){
        List<UcTableInfo> listUcTableInfo = Lists.newArrayList();
        ActionFuture<IndicesStatsResponse> isr = transportClient.admin().indices().stats(new IndicesStatsRequest().all());
        IndicesAdminClient indicesAdminClient = transportClient.admin().indices();
        Map<String, IndexStats> indexStatsMap = isr.actionGet().getIndices();

        Set<String> set = isr.actionGet().getIndices().keySet();
        for(String indexName : set){
            UcTableInfo ucTableInfo = new UcTableInfo();
            ucTableInfo.setTableName(indexName);
            listUcTableInfo.add(ucTableInfo);
        }
        showDebugInfo("返回所有的索引:" + JSONObject.toJSONString(listUcTableInfo));
        return listUcTableInfo;
    }

    public Pair<String,List<UcColumnInfo>> getMapping(TransportClient transportClient, String tableName) throws SQLException {
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(tableName).types(new String[0]);
        GetMappingsResponse getMappingsResponse = transportClient.admin().indices().getMappings(getMappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> immutableOpenMapImmutableOpenMap = getMappingsResponse.getMappings();
        int index = 1;
        tableName = tableName.replace("*","");

FINISH:     for (Iterator<String> it = immutableOpenMapImmutableOpenMap.keysIt(); it.hasNext(); ) {
            String key = it.next();
            if (key.contains(tableName) == false){
                continue;
            }
            ImmutableOpenMap<String, MappingMetaData> immutableOpenMap = immutableOpenMapImmutableOpenMap.get(key);
            if (immutableOpenMap == null) {
                continue;
            }
            for (ObjectCursor<String> objectCursor : immutableOpenMap.keys()) {
                MappingMetaData typeMapping = immutableOpenMap.get(objectCursor.value);
                Map<String, Map<String, String>> mapPropertie = (Map<String, Map<String, String>>) typeMapping.getSourceAsMap().get("properties");
                if (mapPropertie == null) {
                    throw new SQLException("字段不存在");
                }
                for (String field : mapPropertie.keySet()) {
                    Map<String, String> mapFieldInfo = mapPropertie.get(field);
                    String type = mapFieldInfo.get("type");
                    if (StringUtils.isEmpty(type) == true) {
                        type = "keyword";
                    }
                    type = type.toUpperCase();
                    UcColumnInfo ucColumnInfo = new UcColumnInfo(field, UcType.valueOf(type), tableName, tableName, tableName, field, 1000, index);
                    listUcColumnInfo.add(ucColumnInfo);
                    index++;
                }
                break FINISH;
            }
            break;
        }
        showDebugInfo("返回索引的列(" + tableName + "):" + JSONObject.toJSONString(listUcColumnInfo));
        Pair<String,List<UcColumnInfo>> pair = new ImmutablePair<String,List<UcColumnInfo>>(tableName,listUcColumnInfo);
        return pair;
    }

    public Pair<String,List<UcColumnInfo>> getTemlpate(TransportClient transportClient, String tableName) throws SQLException{
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        try {
            GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest();
            getIndexTemplatesRequest.names(tableName);
            GetIndexTemplatesResponse getIndexTemplatesResponse = transportClient.admin().indices().getTemplates(getIndexTemplatesRequest).actionGet();
            List<IndexTemplateMetaData> listIndexTemplateMetaData = getIndexTemplatesResponse.getIndexTemplates();
            int index = 1;
FINISH:     for (IndexTemplateMetaData indexTemplateMetaData : listIndexTemplateMetaData) {
                ImmutableOpenMap<String, CompressedXContent> immutableOpenMap = indexTemplateMetaData.getMappings();
                for (ObjectCursor<String> objectCursor : immutableOpenMap.keys()) {
                    CompressedXContent compressedXContent = immutableOpenMap.get(objectCursor.value);
                    MappingMetaData typeMapping = new MappingMetaData(compressedXContent);
                    Map<String, Map<String, String>> mapPropertie = (Map<String, Map<String, String>>) typeMapping.getSourceAsMap().get("properties");
                    if (mapPropertie == null) {
                        throw new SQLException("字段不存在");
                    }
                    for (String field : mapPropertie.keySet()) {
                        Map<String, String> mapFieldInfo = mapPropertie.get(field);
                        String type = mapFieldInfo.get("type");
                        if (StringUtils.isEmpty(type) == true) {
                            type = "keyword";
                        }
                        type = type.toUpperCase();
                        UcColumnInfo ucColumnInfo = new UcColumnInfo(field, UcType.valueOf(type), tableName, tableName, tableName, field, 1000, index);
                        listUcColumnInfo.add(ucColumnInfo);
                        index++;
                    }
                    break FINISH;
                }
            }
        }catch (IOException ex){
            throw new SQLException("获取索引模板失败");
        }
        showDebugInfo("返回模板的列(" + tableName + "):" + JSONObject.toJSONString(listUcColumnInfo));
        Pair<String,List<UcColumnInfo>> pair = new ImmutablePair<String,List<UcColumnInfo>>(tableName,listUcColumnInfo);
        return pair;
    }


    public boolean getEsHealthStatus(TransportClient transportClient){
        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse response = transportClient.admin().cluster().health(request).actionGet();
        return response.isTimedOut();
    }

    private Object getSqlVarValue(UcSqlVarInfo ucSqlVarInfo, Map<Integer, UcParamInfo> mapParam){
        Object o = ucSqlVarInfo.getVarValue();
        if (ucSqlVarInfo.getVarType() == UcSqlVarInfo.VarType.VARIABLE){
            UcParamInfo ucParamInfo = mapParam.get(ucSqlVarInfo.getVarIndex());
            if (ucParamInfo != null){
                o = ucParamInfo.getValue();
            }
        }
        return o;
    }

    @Override
    public List<UcTableInfo> getTables() {
        List<UcTableInfo> listUcTableInfo = getAllIndex(transportClient);
        return listUcTableInfo;
    }

    @Override
    public Pair<String,List<UcColumnInfo>> getColumnInfo(String tableName) throws SQLException {
        Pair<String, List<UcColumnInfo>> pair = getMapping(transportClient, tableName);
        List<UcColumnInfo> listUcColumnInfo = pair.getRight();
        if (CollectionUtils.isEmpty(listUcColumnInfo) == true) {
            if (tableName.indexOf("*") >= 0) {
                pair = getTemlpate(transportClient, tableName);
            }
        }

        return pair;
    }

    @Override
    public List<UcColumnInfo> getColumnInfo(UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        List<String> listTableName = Lists.newArrayList();
        switch (ucSqlParser.dmlType) {
            case SELECT:
                {
                    String tableName = getTableName(ucSqlParser.getUcSqlSelectVisitor().getListSelectTableName(), mapParam);
                    Pair<String,String> pairTableInfo = getTableAndTypeName(tableName);
                    tableName = pairTableInfo.getLeft();
                    String typeName = pairTableInfo.getRight();
                    if (ucSqlParser.getUcSqlSelectVisitor().isCount() == true) {
                        List<UcSqlVarInfo> listUcSqlVarInfo = ucSqlParser.getUcSqlSelectVisitor().getListSelectColumnName();
                        listUcColumnInfo = getCountColumnInfo(tableName, listUcSqlVarInfo, mapParam);
                    } else {
                        Pair<String,List<UcColumnInfo>> pairColumnInfo = getColumnInfo(tableName);
                        listUcColumnInfo = pairColumnInfo.getRight();
                        List<UcSqlHighlight> listUcSqlHighlight = ucSqlParser.getUcSqlSelectVisitor().getListSelectHighlightColummName();
                        int index = listUcColumnInfo.size();
                        List<UcColumnInfo> listHighlightColummInfo = getHighlightColummInfo(tableName, listUcSqlHighlight,mapParam,index);
                        listUcColumnInfo.addAll(listHighlightColummInfo);
                    }

                }
                break;
            case INSERT:
                {
                    String tableName = getTableName(ucSqlParser.getUcSqlInsertVisitor().getListInsertTableName(), mapParam);
                    Pair<String,String> pairTableInfo = getTableAndTypeName(tableName);
                    tableName = pairTableInfo.getLeft();
                    String typeName = pairTableInfo.getRight();
                    List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();
                    UcSqlVarInfo ucSqlVarInfo = new UcSqlVarInfo(retDmlColumnName, UcSqlVarInfo.VarType.CONSTANT, 0);
                    listUcSqlVarInfo.add(ucSqlVarInfo);
                    listUcColumnInfo = getCountColumnInfo(tableName, listUcSqlVarInfo, mapParam);
                }
                break;
            case DELETE:
                {
                    String tableName = getTableName(ucSqlParser.getUcSqlDeleteVisitor().getListDeleteTableName(), mapParam);
                    Pair<String,String> pairTableInfo = getTableAndTypeName(tableName);
                    tableName = pairTableInfo.getLeft();
                    String typeName = pairTableInfo.getRight();
                    List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();
                    UcSqlVarInfo ucSqlVarInfo = new UcSqlVarInfo(retDmlColumnName, UcSqlVarInfo.VarType.CONSTANT, 0);
                    listUcSqlVarInfo.add(ucSqlVarInfo);
                    listUcColumnInfo = getCountColumnInfo(tableName, listUcSqlVarInfo, mapParam);
                }
            break;
            case UPDATE:
                {
                    String tableName = getTableName(ucSqlParser.getUcSqlUpdateVisitor().getListUpdateTableName(), mapParam);
                    Pair<String,String> pairTableInfo = getTableAndTypeName(tableName);
                    tableName = pairTableInfo.getLeft();
                    String typeName = pairTableInfo.getRight();
                    List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();
                    UcSqlVarInfo ucSqlVarInfo = new UcSqlVarInfo(retDmlColumnName, UcSqlVarInfo.VarType.CONSTANT, 0);
                    listUcSqlVarInfo.add(ucSqlVarInfo);
                    listUcColumnInfo = getCountColumnInfo(tableName, listUcSqlVarInfo, mapParam);
                }
            break;
        }

        return listUcColumnInfo;
    }

    @Override
    public List<Object> execute(UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException{
        List<Object> listObject = Lists.newArrayList();
        switch (ucSqlParser.dmlType){
            case SELECT:
                listObject = queryInfo(transportClient,ucSqlParser,mapParam);
                break;
            case INSERT:
                listObject = insertInfo(transportClient,ucSqlParser,mapParam);
                break;
            case DELETE:
                listObject = deleteInfo(transportClient,ucSqlParser,mapParam);
                break;
            case UPDATE:
                listObject = updateInfo(transportClient,ucSqlParser,mapParam);
                break;
        }
        return listObject;
    }

    List<UcColumnInfo> getHighlightColummInfo(String tableName, List<UcSqlHighlight> listUcSqlHighlight, Map<Integer, UcParamInfo> mapParam, int index){
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        for(UcSqlHighlight ucSqlHighlight : listUcSqlHighlight) {
            index++;
            UcSqlVarInfo rightUcSqlVarInfo = ucSqlHighlight.getRightUcSqlVarInfo();
            Object o = getSqlVarValue(rightUcSqlVarInfo,mapParam);
            String type = "keyword";
            type = type.toUpperCase();
            String field = o.toString();
            UcColumnInfo ucColumnInfo = new UcColumnInfo(field, UcType.valueOf(type), tableName, tableName, tableName, field, 1000, index);
            listUcColumnInfo.add(ucColumnInfo);
        }
        return listUcColumnInfo;
    }

    public List<Object> insertInfo(TransportClient transportClient,UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException{
        List<Object> listObject = Lists.newArrayList();

        String tableName = getTableName(ucSqlParser.getUcSqlInsertVisitor().getListInsertTableName(), mapParam);
        int nFind = tableName.indexOf("*");
        if (nFind >=0){
            throw new SQLException("插入表不能有通配符*:" + tableName);
        }
        //String typeName = getTypeName(transportClient,tableName);

        Pair<String,String> pair = getTableAndTypeName(tableName);
        tableName = pair.getLeft();
        String typeName = pair.getRight();
        List<UcSqlVarInfo> listInsertColumnName = ucSqlParser.getUcSqlInsertVisitor().getListInsertColumnName();
        List<UcSqlVarInfo> listInsertColumnValue = ucSqlParser.getUcSqlInsertVisitor().getListInsertColumnValue();

        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
        int i = 0;
        int count = 0;
        while (i < listInsertColumnValue.size()) {
            try {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                String id = "";
                for (UcSqlVarInfo columnInfo : listInsertColumnName) {
                    String columnName = getSqlVarValue(columnInfo,mapParam).toString();
                    UcSqlVarInfo valueInfo = listInsertColumnValue.get(i);
                    Object o = getSqlVarValue(valueInfo,mapParam);
                    xContentBuilder.field(columnName,o);
                    if (columnName.equals("id") == true){
                        if (o != null) {
                            id = o.toString();
                        }
                    }
                    i++;
                }
                xContentBuilder.endObject();
                IndexRequestBuilder indexRequestBuilder = null;
                if (id.equals("") == true){
                    throw new SQLException("插入表必须有id字段");
                }

                indexRequestBuilder = transportClient.prepareIndex(tableName, typeName, id).setSource(xContentBuilder);
                bulkRequest.add(indexRequestBuilder);
                count++;
            } catch (IOException e) {
                throw new SQLException(e.getMessage());
            }
        }

        BulkResponse bulkResponse = bulkRequest.get();
        boolean bRet = bulkResponse.hasFailures();
        if (bRet == true){
            String message = bulkResponse.buildFailureMessage();
            throw new SQLException(message);
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(retDmlColumnName,count);
        listObject.add(jsonObject);
        return listObject;
    }

    public List<Object> updateInfo(TransportClient transportClient,UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException{
        List<Object> listObject = Lists.newArrayList();

        String tableName = getTableName(ucSqlParser.getUcSqlUpdateVisitor().getListUpdateTableName(), mapParam);
        int nFind = tableName.indexOf("*");
        if (nFind >=0){
            throw new SQLException("修改表不能有通配符*:" + tableName);
        }
        //String typeName = getTypeName(transportClient,tableName);

        Pair<String,String> pair = getTableAndTypeName(tableName);
        tableName = pair.getLeft();
        String typeName = pair.getRight();

        List<UcSqlUpdateInfo> listUcSqlUpdateInfo = ucSqlParser.getUcSqlUpdateVisitor().getListUcSqlUpdateInfo();

        if (CollectionUtils.isEmpty(listUcSqlUpdateInfo) == true){
            throw new SQLException("修改的内容不能为空");
        }

        List<UcSqlWhere> listUcSqlWhere = ucSqlParser.getUcSqlUpdateVisitor().getListUcSqlWhere();
        List<String> listId = getCustomSqlWhere(listUcSqlWhere,mapParam);

        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();

        int count = 0;
        try {
            for (String id : listId) {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                for (UcSqlUpdateInfo ucSqlUpdateInfo : listUcSqlUpdateInfo) {
                    String columnName = getSqlVarValue(ucSqlUpdateInfo.getUpdateColumnName(), mapParam).toString();
                    if (columnName.equals("id") == false) {
                        Object o = getSqlVarValue(ucSqlUpdateInfo.getUpdateColumnValue(), mapParam);
                        xContentBuilder.field(columnName, o);
                    }
                }
                xContentBuilder.field("id",id);
                xContentBuilder.endObject();
                IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(tableName, typeName, id).setSource(xContentBuilder);
                bulkRequest.add(indexRequestBuilder);
                count++;
            }
        }catch (IOException e) {
            throw new SQLException(e.getMessage());
        }

        BulkResponse bulkResponse = bulkRequest.get();
        boolean bRet = bulkResponse.hasFailures();
        if (bRet == true){
            String message = bulkResponse.buildFailureMessage();
            throw new SQLException(message);
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(retDmlColumnName,bulkResponse.getItems().length);
        listObject.add(jsonObject);
        return listObject;
    }

    public List<Object> deleteInfo(TransportClient transportClient,UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException{
        List<Object> listObject = Lists.newArrayList();

        String tableName = getTableName(ucSqlParser.getUcSqlDeleteVisitor().getListDeleteTableName(), mapParam);
        int nFind = tableName.indexOf("*");
        if (nFind >=0){
            throw new SQLException("删除表不能有通配符*:" + tableName);
        }

        //String typeName = getTypeName(transportClient,tableName);
        Pair<String,String> pair = getTableAndTypeName(tableName);
        tableName = pair.getLeft();
        String typeName = pair.getRight();

        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();

        List<UcSqlWhere> listUcSqlWhere = ucSqlParser.getUcSqlDeleteVisitor().getListUcSqlWhere();
        List<String> listId = getCustomSqlWhere(listUcSqlWhere,mapParam);

        for(String id : listId){
            DeleteRequestBuilder deleteRequestBuilder = transportClient.prepareDelete(tableName, typeName, id);
            bulkRequest.add(deleteRequestBuilder);
        }

        BulkResponse bulkResponse = bulkRequest.get();
        boolean bRet = bulkResponse.hasFailures();
        if (bRet == true){
            throw  new SQLException(bulkResponse.buildFailureMessage());
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(retDmlColumnName,bulkResponse.getItems().length);
        listObject.add(jsonObject);
        return listObject;
    }

    List<String> getCustomSqlWhere(List<UcSqlWhere> listUcSqlWhere,Map<Integer, UcParamInfo> mapParam) throws SQLException {
        if (CollectionUtils.isEmpty(listUcSqlWhere) == true){
            throw new SQLException("修改和删除需要匹配条件");
        }

        listUcSqlWhere = listUcSqlWhere.stream().filter(ucSqlWhere -> ucSqlWhere.getSql_token() == UcSqlEnums.SQL_TOKEN.EQ || ucSqlWhere.getSql_token() == UcSqlEnums.SQL_TOKEN.IN).collect(Collectors.toList());

        UcSqlWhere ucSqlWhere = listUcSqlWhere.get(0);
        if (ucSqlWhere.getSql_token() != UcSqlEnums.SQL_TOKEN.EQ && ucSqlWhere.getSql_token() != UcSqlEnums.SQL_TOKEN.IN ){
            throw new SQLException("修改和删除的匹配条件只能是等于和IN");
        }

        if (listUcSqlWhere.size() > 1){
            throw new SQLException("修改和删除只需要一个匹配条件");
        }

        List<String> listId = Lists.newArrayList();
        Object leftObject = getSqlVarValue(ucSqlWhere.getLetfUcSqlVarInfo(),mapParam);
        String columnName = leftObject.toString();
        if (columnName.equals("id") == false){
            throw new SQLException("修改和删除的匹配字段只能是id");
        }

        List<UcSqlVarInfo> listRightUcSqlVarInfo = ucSqlWhere.getListRightUcSqlVarInfo();
        for(UcSqlVarInfo ucSqlVarInfo : listRightUcSqlVarInfo) {
            Object rightObject = getSqlVarValue(ucSqlVarInfo, mapParam);
            listId.add(rightObject.toString());
        }

        if (CollectionUtils.isEmpty(listId) == true){
            throw new SQLException("修改和删除的匹配字段id内容为空");
        }

        return listId;
    }

    @Override
    public Object getColumnValue(Object o, String columnName){
        Object retObject = null;
        if (o instanceof  JSONObject){
            JSONObject jsonObject = (JSONObject) o;
            retObject = jsonObject.get(columnName);
        }else {
            JSONObject jsonObject = JSONObject.parseObject(o.toString());
            retObject = jsonObject.get(columnName);
        }
        return retObject;
    }


    @Override
    public Object getOperObject() {
        return transportClient;
    }

    @Override
    public boolean init(Properties properties) throws SQLException, URISyntaxException {
        String url = properties.getProperty("url");
        if (StringUtils.isEmpty(url) == true){
            throw new SQLException("ES连配置错误:" + url);
        }
        if (url.startsWith(prefixJdbc) != true){
            throw new SQLException("ES连配置错误:" + url);
        }

        String tempUrl = url.replace(prefixJdbc,esPrefix);
        URI uri = new URI(tempUrl);

        String tcpEsHost = uri.getAuthority();
        String clusterName = uri.getPath();
        clusterName = clusterName.replace("/","");

        String params = uri.getQuery();
        if (StringUtils.isEmpty(params) == false) {
            Map<String, String> config = Splitter.on("&").withKeyValueSeparator("=").split(params);
            if (config.containsKey("debug") == true) {
                bDebug = Boolean.valueOf(config.get("debug"));
                showDebugInfo("uc-db(EsOper)启动调试日志");
            }
        }

        String validationQueryName = "validationQuery";
        String sql = properties.getProperty(validationQueryName);
        if (StringUtils.isEmpty(sql) == false){
            UcSqlParser ucSqlParser = new UcSqlParser();
            boolean bParser = ucSqlParser.parserSql(sql);
            if (bParser == false){
                throw new SQLException(ucSqlParser.errorMsg);
            }
        }

        Settings.Builder settingBuilder = Settings.builder();
        settingBuilder.put("cluster.name",clusterName);

        String [] arrayIpAddress = tcpEsHost.split(",");

        List<TransportAddress> listTransportAddress = Lists.newArrayList();
        for(String ipAndPort : arrayIpAddress){
            if (StringUtils.isEmpty(ipAndPort) == true){
                continue;
            }
            String []arrayTemp = ipAndPort.split(":");
            if (arrayTemp.length != 2){
                continue;
            }
            String ipAddress = arrayTemp[0];
            InetAddress ip = null;
            try {
                ip = InetAddress.getByName(ipAddress);
            } catch (UnknownHostException ex) {
                throw new SQLException("获取服务器失败");
            }
            int nPort = Integer.parseInt(arrayTemp[1]);
            TransportAddress transportAddress = new TransportAddress(ip,nPort);
            listTransportAddress.add(transportAddress);
        }
        TransportAddress[] arrayTransportAddress = listTransportAddress.stream().toArray(TransportAddress[]::new);
        Settings settings = settingBuilder.build();
        transportClient = new PreBuiltTransportClient(settings).addTransportAddresses(arrayTransportAddress);
        boolean bHealth = getEsHealthStatus(transportClient);
        if (bHealth == true){
            closed = true;
            throw new SQLException("连接服务器心跳失败");
        }
        closed = false;
        return closed;
    }

    @Override
    public boolean close() {
        if (transportClient != null) {
            transportClient.close();
        }
        closed = true;
        return closed;
    }

    @Override
    public boolean checkConnection() {
        boolean bHealth = getEsHealthStatus(transportClient);
        return bHealth;
    }

    public TransportClient getTransportClient() {
        return transportClient;
    }

    public List<UcColumnInfo> getCountColumnInfo(String tableName,List<UcSqlVarInfo> listUcSqlVarInfo,Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        int index = 1;
        UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
        String columnName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
        String type = Long.class.getSimpleName().toUpperCase();
        UcColumnInfo ucColumnInfo = new UcColumnInfo(columnName, UcType.valueOf(type), tableName, tableName, tableName, columnName, 1000, index);
        listUcColumnInfo.add(ucColumnInfo);
        return listUcColumnInfo;
    }

    Pair<String,String> getTableAndTypeName(String indexName) throws SQLException {

        String [] arrayString = indexName.split("\\.");
        String tableName = "";
        String typeName = "";
        if (arrayString.length <2){
            tableName = indexName;
        }else{
            tableName = arrayString[0];
            typeName = arrayString[1];
        }
        Pair<String,String> pair = new ImmutablePair<String,String>(tableName,typeName);
        return pair;
    }

    String getTypeName(TransportClient transportClient,String indexName) throws SQLException {
        String typeName = "";

        IndicesAdminClient indicesAdminClient = transportClient.admin().indices();
        GetMappingsResponse getMappingsResponse = null;
        try {
            getMappingsResponse = indicesAdminClient.getMappings(new GetMappingsRequest().indices(indexName)).get();
            ImmutableOpenMap<String, MappingMetaData> mapping = getMappingsResponse.mappings().get(indexName);
            for (ObjectObjectCursor<String, MappingMetaData> objectObjectCursor : mapping) {
                typeName = objectObjectCursor.key;
            }
        } catch (Exception e) {
            throw new SQLException("表不存在(无法获取ES索引类型)");
        }
        return typeName;
    }
}
