package com.uc.mongodb.oper;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.uc.base.enums.UcSqlEnums;
import com.uc.base.jdbc.UcColumnInfo;
import com.uc.base.jdbc.UcParamInfo;
import com.uc.base.jdbc.UcTableInfo;
import com.uc.base.oper.DbOper;
import com.uc.base.parser.SqlTreeNode;
import com.uc.base.parser.UcSqlParser;
import com.uc.base.sql.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.uc.base.sql.UcSqlVarInfo.VarType.CONSTANT;

public class MongoDbOper implements DbOper {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbOper.class);

    private String dbName = "";
    private String mongodbPrefix = "mongodb://";
    private MongoClient mongoClient = null;
    private boolean closed = true;
    private String prefixJdbc = "mongodb:mysql://";
    private static String retDmlColumnName = "count(0)";

    public static List<Object> queryInfo(MongoClient mongoClient, String dbName, UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<Object> listObject = Lists.newArrayList();
        FindIterable<Document> findIterable = null;
        String tableName = getTableName(ucSqlParser.getUcSqlSelectVisitor().getListSelectTableName(),mapParam);

        MongoCollection mongoCollection = getCollection(mongoClient,dbName,tableName);

        String fields = addField( ucSqlParser.getUcSqlSelectVisitor().getListSelectColumnName(),mapParam);
        Bson filter = addWhere(mongoClient,ucSqlParser.getUcSqlSelectVisitor().getListRootWhereSqlTreeNode(),mapParam);
        Bson sort = addSortBuilder(mongoClient,ucSqlParser.getUcSqlSelectVisitor().getListSelectOrderBy(),mapParam);
        Pair<Integer, Integer> limit = addFormSize(mongoClient,ucSqlParser.getUcSqlSelectVisitor().getListSelectLimit(),mapParam);
        if (ucSqlParser.getUcSqlSelectVisitor().isCount() == true){
            long count = 0;
            if (filter != null) {
                count = mongoCollection.countDocuments(filter);
            }else{
                count = mongoCollection.countDocuments();
            }
            UcSqlVarInfo ucSqlVarInfo = ucSqlParser.getUcSqlSelectVisitor().getListSelectColumnName().get(0);
            String columnName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();

            Document document = new Document();
            document.put(columnName,count);

            listObject.add(document);
        }else {
            if (filter != null){
                findIterable = mongoCollection.find(filter);
            }else{
                findIterable = mongoCollection.find();
            }
            findIterable.sort(sort);
            if (limit != null) {
                findIterable = findIterable.skip(limit.getLeft()).limit(limit.getRight()).sort(sort);
            }

            for (Document document : findIterable) {
                listObject.add(document);
            }
        }
        return listObject;
    }

    public static List<Object> insertInfo(MongoClient mongoClient,String dbName,UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<Object> listObject = Lists.newArrayList();

        String tableName = getTableName(ucSqlParser.getUcSqlInsertVisitor().getListInsertTableName(), mapParam);
        MongoCollection mongoCollection = getCollection(mongoClient,dbName,tableName);
        List<UcSqlVarInfo> listInsertColumnName = ucSqlParser.getUcSqlInsertVisitor().getListInsertColumnName();
        List<UcSqlVarInfo> listInsertColumnValue = ucSqlParser.getUcSqlInsertVisitor().getListInsertColumnValue();

        int i = 0;
        int count = 0;
        List<Document> listDocument = Lists.newArrayList();
        while (i < listInsertColumnValue.size()) {
            Document insertDocument = new Document();
            for (UcSqlVarInfo columnInfo : listInsertColumnName) {
                String columnName = getSqlVarValue(columnInfo,mapParam).toString();
                UcSqlVarInfo valueInfo = listInsertColumnValue.get(i);
                Object o = getSqlVarValue(valueInfo,mapParam);
                insertDocument.put(columnName,o);
                i++;
            }
            listDocument.add(insertDocument);
            count++;
        }
        mongoCollection.insertMany(listDocument);

        Document document = new Document();
        document.put(retDmlColumnName,count);
        listObject.add(document);
        return listObject;
    }

    public static List<Object> updateInfo(MongoClient mongoClient,String dbName,UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<Object> listObject = Lists.newArrayList();

        String tableName = getTableName(ucSqlParser.getUcSqlUpdateVisitor().getListUpdateTableName(), mapParam);
        MongoCollection mongoCollection = getCollection(mongoClient,dbName,tableName);

        List<UcSqlUpdateInfo> listUcSqlUpdateInfo = ucSqlParser.getUcSqlUpdateVisitor().getListUcSqlUpdateInfo();

        if (CollectionUtils.isEmpty(listUcSqlUpdateInfo) == true){
            throw new SQLException("修改的内容不能为空");
        }
        Map<String,Object> mapUpdate = Maps.newHashMap();

        for (UcSqlUpdateInfo ucSqlUpdateInfo : listUcSqlUpdateInfo) {
            String columnName = getSqlVarValue(ucSqlUpdateInfo.getUpdateColumnName(),mapParam).toString();
            Object o = getSqlVarValue(ucSqlUpdateInfo.getUpdateColumnValue(),mapParam);
            mapUpdate.put(columnName,o);
        }

        Document updateDocument = new Document();
        updateDocument.put("$set",mapUpdate);

        Bson filter = addWhere(mongoClient,ucSqlParser.getUcSqlUpdateVisitor().getListRootWhereSqlTreeNode(),mapParam);
        if (filter == null){
            throw new SQLException("修改的过滤条件不能为空");
        }
        UpdateResult updateResult = mongoCollection.updateMany(filter,updateDocument);
        Document document = new Document();
        document.put(retDmlColumnName,(int)updateResult.getMatchedCount());
        listObject.add(document);
        return listObject;
    }

    public static List<Object> deleteInfo(MongoClient mongoClient,String dbName,UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<Object> listObject = Lists.newArrayList();

        String tableName = getTableName(ucSqlParser.getUcSqlDeleteVisitor().getListDeleteTableName(), mapParam);
        MongoCollection mongoCollection = getCollection(mongoClient,dbName,tableName);

        Map<String,Object> mapUpdate = Maps.newHashMap();

        Bson filter = addWhere(mongoClient,ucSqlParser.getUcSqlDeleteVisitor().getListRootWhereSqlTreeNode(),mapParam);
        if (filter == null){
            throw new SQLException("删除的过滤条件不能为空");
        }
        DeleteResult deleteResult = mongoCollection.deleteMany(filter);
        Document document = new Document();
        document.put(retDmlColumnName,(int)deleteResult.getDeletedCount());
        listObject.add(document);
        return listObject;
    }

    private static MongoCollection getCollection(MongoClient mongoClient,String dbName, String documentName) throws SQLException{
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
        if (mongoDatabase == null){
            throw new SQLException("数据库不存在:" + dbName);
        }
        MongoCollection mongoCollection = mongoDatabase.getCollection(documentName);
        if (mongoCollection == null){
            throw new SQLException("文档不存在:" + documentName);
        }
        return mongoCollection;
    }

    static String getQueryTableName(MongoClient mongoClient,String dbName,String tableName) throws SQLException {
        if (StringUtils.isBlank(tableName) == true){
            throw new SQLException("查询表名不能为空 ");
        }

        return tableName;
    }

    static Bson addSortBuilder(MongoClient mongoClient, List<UcSqlOrderBy> listOrderBy, Map<Integer, UcParamInfo> mapParam){
        Document sortDocument = new Document();
        for(UcSqlOrderBy ucSqlOrderBy : listOrderBy){
            Bson bson = null;
            UcSqlVarInfo columnVar = ucSqlOrderBy.getColumnVar();
            String columnName = getSqlVarValue(columnVar,mapParam).toString();
            UcSqlVarInfo sortVar = ucSqlOrderBy.getSortVar();
            String sort = getSqlVarValue(sortVar,mapParam).toString();
            sort = sort.toUpperCase();
            if ("ASC".equals(sort) == true){
                sortDocument.put(columnName,1);
            }else {
                sortDocument.put(columnName,-1);
            }
        }
        return sortDocument;
    }

    static Pair<Integer, Integer> addFormSize(MongoClient mongoClient, List<UcSqlLimit> listLimit, Map<Integer, UcParamInfo> mapParam){
        if (listLimit.size() == 0){
            return null;
        }
        UcSqlLimit ucSqlLimit = listLimit.get(0);
        UcSqlVarInfo offsetVar = ucSqlLimit.getOffsetVar();
        UcSqlVarInfo sizeVar = ucSqlLimit.getSizeVar();
        int offset = Integer.parseInt(getSqlVarValue(offsetVar,mapParam).toString());
        int size = Integer.parseInt(getSqlVarValue(sizeVar,mapParam).toString());
        Pair<Integer, Integer> pair = new ImmutablePair<Integer, Integer>(offset,size);
        return pair;
    }

    static String getTableName(List<UcSqlVarInfo> listTableName, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<String> listRet = Lists.newArrayList();
        if (listTableName.size() == 0){
            throw new SQLException("查询表名不能为空 ");
        }

        if (listTableName.size() > 1){
            throw new SQLException("不能多表查询");
        }
        UcSqlVarInfo ucSqlVarInfo = listTableName.get(0);
        String tableName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
        return tableName;
    }

    static String addField(List<UcSqlVarInfo> listSelectColumName, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        String fields = "";
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
        for(String columnName : listColumnName){
            fields = fields + "{'" + columnName + "':1}";
        }
        return fields;
    }

    static Bson addWhere(MongoClient mongoClient, List<List<SqlTreeNode>> listRootWhereSqlTreeNode,Map<Integer, UcParamInfo> mapParam) throws SQLException {
        if (CollectionUtils.isEmpty(listRootWhereSqlTreeNode) == true){
            return null;
        }
        Bson rootBson = null;
        Map<SqlTreeNode,Bson> mapBson = Maps.newHashMap();
        int size = listRootWhereSqlTreeNode.size();
        for(int i = size-1; i>=0; i--){
            List<SqlTreeNode> listSqlTreeNode = listRootWhereSqlTreeNode.get(i);
            for(SqlTreeNode sqlTreeNode : listSqlTreeNode){
                SqlTreeNode leftSqlTreeNode = sqlTreeNode.getLeftChildSqlTreeNode();
                SqlTreeNode rightSqlTreeNode = sqlTreeNode.getRightChildSqlTreeNode();
                List<Bson> listBson = Lists.newArrayList();
                if (leftSqlTreeNode != null){
                    Bson leftBson = getBson(leftSqlTreeNode,mapParam,mapBson);
                    listBson.add(leftBson);
                }
                if (rightSqlTreeNode != null){
                    Bson rightBson = getBson(rightSqlTreeNode,mapParam,mapBson);
                    listBson.add(rightBson);
                }
                rootBson = getBson(sqlTreeNode,listBson,mapBson);
            }
        }
        return rootBson;
    }

    static Bson getBson(SqlTreeNode sqlTreeNode,List<Bson> listBson,Map<SqlTreeNode,Bson> mapBson){
        Bson bson = mapBson.get(sqlTreeNode);
        if (bson == null){
            UcSqlWhere ucSqlWhere = sqlTreeNode.getUcSqlWhere();
            UcSqlEnums.SQL_TOKEN sql_token = ucSqlWhere.getSql_token();
            if (sql_token == UcSqlEnums.SQL_TOKEN.AND){
                bson = Filters.and(listBson);
            }else if (sql_token == UcSqlEnums.SQL_TOKEN.OR){
                bson = Filters.or(listBson);
            }
            mapBson.put(sqlTreeNode,bson);
        }
        return bson;
    }

    static Bson getBson(SqlTreeNode sqlTreeNode,Map<Integer, UcParamInfo> mapParam,Map<SqlTreeNode,Bson> mapBson) throws SQLException {
        Bson bson = mapBson.get(sqlTreeNode);
        if (bson == null){
            UcSqlWhere ucSqlWhere = sqlTreeNode.getUcSqlWhere();
            UcSqlEnums.SQL_TOKEN sql_token = ucSqlWhere.getSql_token();
            if (sql_token == UcSqlEnums.SQL_TOKEN.AND){
                bson = Filters.and();
            }else if (sql_token == UcSqlEnums.SQL_TOKEN.OR){
                bson = Filters.or();

            }else {
                bson = getQuery(ucSqlWhere, mapParam);
            }
            mapBson.put(sqlTreeNode,bson);
        }
        return bson;
    }

    static Bson getQuery(UcSqlWhere ucSqlWhere,Map<Integer, UcParamInfo> mapParam) throws SQLException {
        UcSqlEnums.SQL_TOKEN sql_token = ucSqlWhere.getSql_token();
        UcSqlVarInfo leftUcSqlVarInfo = ucSqlWhere.getLetfUcSqlVarInfo();
        List<UcSqlVarInfo> listUcSqlVarInfo = ucSqlWhere.getListRightUcSqlVarInfo();
        String columnName = getSqlVarValue(leftUcSqlVarInfo,mapParam).toString();
        Bson query = null;
        switch (sql_token){
            case EQ:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                query = Filters.eq(columnName,columnValue);
            }
            break;
            case NE:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                query = Filters.ne(columnName,columnValue);
            }
            break;
            case GT:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                query = Filters.gt(columnName,columnValue);
            }
            break;
            case GTE:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                query = Filters.gte(columnName,columnValue);
            }
            break;
            case LT:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                query = Filters.lt(columnName,columnValue);
            }
            break;
            case LTE:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                query = Filters.lte(columnName,columnValue);
            }
            break;
            case LIKE:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam).toString();
                query = Filters.eq(columnName,columnValue);

            }
            break;
            case NOT_LIKE:
            {
                UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
                Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam).toString();
                query = Filters.ne(columnName,columnValue);
            }
            break;
            case IN:
            {
                List<Bson> listQueryFilter = Lists.newArrayList();
                for(UcSqlVarInfo ucSqlVarInfo : listUcSqlVarInfo) {
                    Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                    Bson queryTemp = Filters.eq(columnName,columnValue);
                    listQueryFilter.add(queryTemp);
                }
                query = Filters.or(listQueryFilter);
            }
            break;
            case NOT_IN:
            {
                List<Bson> listQueryFilter = Lists.newArrayList();
                for(UcSqlVarInfo ucSqlVarInfo : listUcSqlVarInfo) {
                    Object columnValue = getSqlVarValue(ucSqlVarInfo, mapParam);
                    Bson queryTemp = Filters.ne(columnName,columnValue);
                    listQueryFilter.add(queryTemp);
                }
                query = Filters.or(listQueryFilter);
            }
            break;
            case BETWEEN:
            {
                UcSqlVarInfo startSqlVarInfo = listUcSqlVarInfo.get(0);
                UcSqlVarInfo endSqlVarInfo = listUcSqlVarInfo.get(1);
                Object startValue = getSqlVarValue(startSqlVarInfo, mapParam);
                Object endValue = getSqlVarValue(endSqlVarInfo, mapParam);
                query = Filters.and(Filters.gte(columnName,startValue),Filters.lte(columnName,endValue));
            }
            break;
            default:
            {
                throw new SQLException("不支持的查询"+ sql_token.getName());
            }
        }
        return query;
    }

    private static Object getSqlVarValue(UcSqlVarInfo ucSqlVarInfo, Map<Integer, UcParamInfo> mapParam){
        Object o = ucSqlVarInfo.getVarValue();
        if (ucSqlVarInfo.getVarType() == UcSqlVarInfo.VarType.VARIABLE){
            UcParamInfo ucParamInfo = mapParam.get(ucSqlVarInfo.getVarIndex());
            if (ucParamInfo != null){
                o = ucParamInfo.getValue();
            }
        }
        return o;
    }

    public static  Pair<String,List<UcColumnInfo>> getColumnInfo(MongoClient mongoClient, String dbName, String tableName) throws SQLException {
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(tableName);
        int index = 1;
        Document document = mongoCollection.find().first();
        if (document != null) {
            for (String fieldName : document.keySet()) {
                Object object = document.get(fieldName);
                String type = object.getClass().getSimpleName().toUpperCase();
                UcColumnInfo ucColumnInfo = new UcColumnInfo(fieldName, UcType.valueOf(type), tableName, tableName, tableName, fieldName, 1000, index);
                listUcColumnInfo.add(ucColumnInfo);
                index++;
            }
        }
        Pair<String,List<UcColumnInfo>> pair = new ImmutablePair<String,List<UcColumnInfo>>(tableName,listUcColumnInfo);
        return pair;
    }

    public static  List<UcColumnInfo> getSelectColumnInfo(List<UcSqlVarInfo> listUcSqlVarInfo,List<UcColumnInfo> listUcColumnInfo,Map<Integer, UcParamInfo> mapParam) throws SQLException {
        listUcColumnInfo = listUcColumnInfo.stream()
                .map(
                        ucColumnInfo -> listUcSqlVarInfo.stream()
                        .filter(
                                ucSqlVarInfo -> ucColumnInfo.name.equals(getSqlVarValue(ucSqlVarInfo,mapParam).toString())
                        )
                        .findAny()
                        .map(ucSqlVarInfo -> {
                            return ucColumnInfo;
                        }).orElse(null)).filter(Objects::nonNull)
                .collect(Collectors.toList());
        return listUcColumnInfo;
    }

    public static  List<UcColumnInfo> getCountColumnInfo(String tableName,List<UcSqlVarInfo> listUcSqlVarInfo,Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        int index = 1;
        UcSqlVarInfo ucSqlVarInfo = listUcSqlVarInfo.get(0);
        String columnName = getSqlVarValue(ucSqlVarInfo,mapParam).toString();
        String type = Long.class.getSimpleName().toUpperCase();
        UcColumnInfo ucColumnInfo = new UcColumnInfo(columnName, UcType.valueOf(type), tableName, tableName, tableName, columnName, 1000, index);
        listUcColumnInfo.add(ucColumnInfo);
        return listUcColumnInfo;
    }

    @Override
    public List<UcTableInfo> getTables() throws SQLException {
        List<UcTableInfo> listUcTableInfo = Lists.newArrayList();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
        if (mongoDatabase == null){
            throw new SQLException("数据库不存在:" + dbName);
        }
        MongoIterable<String> mongoIterable = mongoDatabase.listCollectionNames();
        for(String name : mongoIterable){
            UcTableInfo ucTableInfo = new UcTableInfo();
            ucTableInfo.setTableName(name);
            listUcTableInfo.add(ucTableInfo);
        }
        return listUcTableInfo;
    }

    @Override
    public Pair<String,List<UcColumnInfo>> getColumnInfo(String tableName) throws SQLException {
        Pair<String,List<UcColumnInfo>> pair = getColumnInfo(mongoClient,dbName,tableName);
        return pair;
    }

    @Override
    public List<UcColumnInfo> getColumnInfo(UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        List<UcSqlVarInfo> listTableName = Lists.newArrayList();
        switch (ucSqlParser.dmlType){
            case SELECT:
                {
                    listTableName = ucSqlParser.getUcSqlSelectVisitor().getListSelectTableName();
                    String tableName = getTableName(listTableName, mapParam);
                    if (ucSqlParser.getUcSqlSelectVisitor().isCount() == false) {
                        Pair<String,List<UcColumnInfo>> pair = getColumnInfo(mongoClient, dbName, tableName);
                        listUcColumnInfo = pair.getRight();
                        List<UcSqlVarInfo> listUcSqlVarInfo = ucSqlParser.getUcSqlSelectVisitor().getListSelectColumnName();
                        listUcColumnInfo = getSelectColumnInfo(listUcSqlVarInfo, listUcColumnInfo, mapParam);


                    } else {
                        List<UcSqlVarInfo> listUcSqlVarInfo = ucSqlParser.getUcSqlSelectVisitor().getListSelectColumnName();
                        listUcColumnInfo = getCountColumnInfo(tableName, listUcSqlVarInfo, mapParam);
                    }
                }
                break;
            case INSERT:
                {
                    listTableName = ucSqlParser.getUcSqlInsertVisitor().getListInsertTableName();
                    String tableName = getTableName(listTableName, mapParam);
                    List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();
                    UcSqlVarInfo ucSqlVarInfo = new UcSqlVarInfo(retDmlColumnName, CONSTANT, 0);
                    listUcSqlVarInfo.add(ucSqlVarInfo);
                    listUcColumnInfo = getCountColumnInfo(tableName, listUcSqlVarInfo, mapParam);
                }
                break;
            case DELETE:
                {
                    listTableName = ucSqlParser.getUcSqlDeleteVisitor().getListDeleteTableName();
                    String tableName = getTableName(listTableName, mapParam);
                    List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();
                    UcSqlVarInfo ucSqlVarInfo = new UcSqlVarInfo(retDmlColumnName, CONSTANT, 0);
                    listUcSqlVarInfo.add(ucSqlVarInfo);
                    listUcColumnInfo = getCountColumnInfo(tableName, listUcSqlVarInfo, mapParam);
                }
                break;
            case UPDATE:
                {
                    listTableName = ucSqlParser.getUcSqlUpdateVisitor().getListUpdateTableName();
                    String tableName = getTableName(listTableName, mapParam);
                    List<UcSqlVarInfo> listUcSqlVarInfo = Lists.newArrayList();
                    UcSqlVarInfo ucSqlVarInfo = new UcSqlVarInfo(retDmlColumnName, CONSTANT, 0);
                    listUcSqlVarInfo.add(ucSqlVarInfo);
                    listUcColumnInfo = getCountColumnInfo(tableName, listUcSqlVarInfo, mapParam);
                }
                break;
        }

        return listUcColumnInfo;
    }

    @Override
    public List<Object> execute(UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException {
        List<Object> listObject = Lists.newArrayList();
        switch (ucSqlParser.dmlType){
            case SELECT:
                listObject = queryInfo(mongoClient,dbName,ucSqlParser,mapParam);;
                break;
            case INSERT:
                listObject = insertInfo(mongoClient,dbName,ucSqlParser,mapParam);;
                break;
            case UPDATE:
                listObject = updateInfo(mongoClient,dbName,ucSqlParser,mapParam);;
                break;
            case DELETE:
                listObject = deleteInfo(mongoClient,dbName,ucSqlParser,mapParam);;
                break;
        }
        return listObject;
    }

    @Override
    public Object getColumnValue(Object o, String columnName) {
        Object retObject = null;
        if (o instanceof Document) {
            Document document = (Document) o;
            retObject = document.get(columnName);
        }else{
            JSONObject jsonObject = JSONObject.parseObject(o.toString());
            retObject = jsonObject.get(columnName);
        }

        return retObject;
    }

    @Override
    public Object getOperObject() {
        return mongoClient;
    }

    @Override
    public boolean init(Properties properties) throws SQLException, URISyntaxException {
        String url = properties.getProperty("url");
        if (StringUtils.isEmpty(url) == true){
            throw new SQLException("MongoDb连配置错误:" + url);
        }
        if (url.startsWith(prefixJdbc) != true){
            throw new SQLException("MongoDb连配置错误:" + url);
        }

        String tempUrl = url.replace(prefixJdbc,mongodbPrefix);

        URI uri = new URI(tempUrl);
        dbName = uri.getPath();

        dbName = dbName.replace("/","");

        MongoClientURI mongoClientURI = new MongoClientURI(tempUrl);
        mongoClient = new MongoClient(mongoClientURI);
        if (mongoClient == null){
            throw new SQLException("连接服务器失败");
        }
        boolean bCheckConnection = checkConnection();
        closed = false;
        return closed;
    }

    @Override
    public boolean close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        closed = true;
        return closed;
    }

    @Override
    public boolean checkConnection() throws SQLException {
        boolean bCheckConnection = false;
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
        if (mongoDatabase == null){
            throw new SQLException("数据库不存在:" + dbName);
        }
        return bCheckConnection = true;
    }



    public static void main(String[] args) throws URISyntaxException, SQLException {
        String tempUrl = "mongodb://admin:adsminasdfj@10.32.16.22:22001,10.32.16.22:22002,10.32.16.22:22003/vk_wechat?authSource=admin";
        URI uri = new URI(tempUrl);
        String path = uri.getPath();
        MongoClientURI mongoClientURI = new MongoClientURI(tempUrl);
        MongoClient mongoClient = new MongoClient(mongoClientURI);
        CodecRegistry codecRegistry = mongoClient.getMongoClientOptions().getCodecRegistry();
        MongoDatabase mongoDatabase = mongoClient.getDatabase("vk_wechat");
        MongoIterable<String> mongoIterable = mongoDatabase.listCollectionNames();
        String queryTableName = "";
        for(String tableName : mongoIterable){
            System.out.println("表名:"+ tableName);
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(tableName);
            Document document = mongoCollection.find().first();
            for(String fieldName : document.keySet()) {
                Object object = document.get(fieldName);
                System.out.println("列名:" + fieldName + ",字段类型:" + object.getClass().getSimpleName());
            }
            queryTableName = tableName;
        }
        queryTableName = "wxwork_msg";
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(queryTableName);
        ObjectId objectId = new ObjectId("5ffe63c983c78c0017dbb8a9");
        Bson filter = Filters.eq("_id",objectId);
        Bson logicFilter = Filters.and(filter);
        BsonDocument tempDocument = filter.toBsonDocument(BsonDocument.class,MongoClient.getDefaultCodecRegistry());
        String temp = "";
        temp = logicFilter.toString();
        temp = filter.toString();
        temp = JSONObject.toJSONString(objectId);
        temp = tempDocument.toJson();
        //queryDocument.put("_id",new ObjectId("5ffe63c983c78c0017dbb8a9"));
        Document queryDocument = Document.parse(temp);
        queryDocument = new Document();
        Bson sort = Sorts.descending("dateTime");

        FindIterable<Document> findIterable = mongoCollection.find().sort(sort).limit(10);
        for(Document document : findIterable){
            System.out.println(document.toJson());
        }
        //5f682503e4b064ac6b113188
        //604b18d80cf2b12f4bb585a0
        String sql = "select * from weixin where (_id ='604b18d80cf2b12f4bb585a0' or _id = '5f682503e4b064ac6b113188' or _id = '739652294556708508') and  (talkerId=29 and friendTypeYunke=2), limit 10";
        //sql = "select * from wxwork_msg where msgid='09cb8cf7-8068-4150-ba57-791b1a53afd2' limit 10;";
        //sql = "update wxwork_msg set `from`='wangxiaobo' where msgid='09cb8cf7-8068-4150-ba57-791b1a53afd2' limit 10;";
        sql = "select * from wxwork_msg  where msgid='09cb8cf7-8068-4150-ba57-791b1a53afd2' limit 10;";
        UcSqlParser ucSqlParser = new UcSqlParser();
        ucSqlParser.parserSql(sql);
        Properties properties = new Properties();
        properties.put("url","mongodb:mysql://admin:adsminasdfj@10.32.16.22:22001,10.32.16.22:22002,10.32.16.22:22003/vk_wechat?authSource=admin");
        MongoDbOper mongoDbOper = new MongoDbOper();
        mongoDbOper.init(properties);
        List<Object> listObject = mongoDbOper.execute(ucSqlParser, Maps.newHashMap());


        for(Object object : listObject){
            Object o = mongoDbOper.getColumnValue(object,"msgid");
            String msgid = (o == null ? "":o.toString());
            o = mongoDbOper.getColumnValue(object,"corpId");
            String corpId = (o == null ? "":o.toString());
            o = mongoDbOper.getColumnValue(object,"roomid");
            String roomid = (o == null ? "":o.toString());
            o = mongoDbOper.getColumnValue(object,"seq");
            String seq = (o == null ? "":o.toString());
            o = mongoDbOper.getColumnValue(object,"from");
            String from = (o == null ? "":o.toString());
            System.out.println(msgid + "|" + corpId + "|" + roomid + "|" + seq + "|" + from);
        }
        System.out.println("end");
    }
}
