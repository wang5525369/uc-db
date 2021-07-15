package com.uc.db.datasource;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.uc.db.constant.UcDbConstants;
import com.uc.es.jdbc.UcEsDataSource;
import com.uc.mongodb.jdbc.UcMongoDbDataSource;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.xpack.sql.jdbc.EsDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxiaobo on 2018/9/6.
 */
public class UcDataSourceOper {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static String defaultDataSourceName = "";
    private static Map mapDataSourceProperties = Maps.newConcurrentMap();
    private static Map mapDataSource = Maps.newConcurrentMap();

    public DataSource connectDataSource(String dataSourceName,Properties properties) throws SQLException, IOException, URISyntaxException {

        Properties propertiesTemp = (Properties) properties.clone();
        propertiesTemp.remove("password");
        propertiesTemp.remove("druid.password");
        logger.info("数据库连接属性：{}", JSONObject.toJSONString(propertiesTemp));

        DataSource dataSource = null;
        String type = properties.getProperty("type");
        if (StringUtils.isBlank(type) == true) {
            type = properties.getProperty("druid.type");
        }
        type = type.toLowerCase();
        if (type.equals(UcDbConstants.DURID_TYPE_NAME) == true) {
            dataSource = connectDruidDataSource(properties);
        } else if (type.equals(UcDbConstants.ES_TYPE_NAME) == true){
            dataSource = connectEsDataSource(properties);
        }else if (type.equals(UcDbConstants.UC_ES_TYPE_NAME) == true){
            dataSource = connectUcEsDataSource(properties);
        }else if (type.equals(UcDbConstants.UC_MONGODB_TYPE_NAME) == true){
            dataSource = connectUcMongoDbDataSource(properties);
        }
        mapDataSourceProperties.put(dataSourceName,properties);
        mapDataSource.put(dataSourceName, dataSource);
        if (StringUtils.isBlank(defaultDataSourceName) == true) {
            defaultDataSourceName = dataSourceName;
        }

        logger.info("数据库连接成功 连接属性：{}", JSONObject.toJSONString(propertiesTemp));
        return dataSource;
    }

    private DataSource connectDruidDataSource(Properties properties) throws SQLException, IOException{
        UcDruidDataSource ucDruidDataSource = new UcDruidDataSource();
        ucDruidDataSource.init(properties);
        return ucDruidDataSource;
    }

    private DataSource connectEsDataSource(Properties properties) throws SQLException, IOException{
        String url = properties.getProperty("url");
        EsDataSource esDataSource = new EsDataSource();
        //esDataSource.setProperties(properties);
        esDataSource.setUrl(url);
        return esDataSource;
    }

    private DataSource connectUcEsDataSource(Properties properties) throws SQLException, IOException, URISyntaxException {
        UcEsDataSource ucEsDataSource = new UcEsDataSource();
        ucEsDataSource.init(properties);
        return ucEsDataSource;
    }

    private DataSource connectUcMongoDbDataSource(Properties properties) throws SQLException, IOException, URISyntaxException {
        UcMongoDbDataSource ucMongoDbDataSource = new UcMongoDbDataSource();
        ucMongoDbDataSource.init(properties);
        return ucMongoDbDataSource;
    }

    public static String getDefaultDataSourceName() {
        return defaultDataSourceName;
    }

    public static Map getMapDataSourceProperties() {
        return mapDataSourceProperties;
    }

    public static Map getMapDataSource() {
        return mapDataSource;
    }
}
