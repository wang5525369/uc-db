package com.uc.db.datasource;

import com.uc.db.common.UcDataSourceImport;
import com.uc.db.config.UcDataSourceConfig;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by wangxiaobo on 2018/1/19.
 */
public class UcMultiDataSource {
    private static final Logger logger = LoggerFactory.getLogger(UcMultiDataSource.class);


    @Autowired
    UcDataSourceConfig ucDataSourceConfig;

    UcDataSourceOper ucDataSourceOper = new UcDataSourceOper();

    /**
     * 动态数据源: 通过AOP在不同数据源之间动态切换
     * @return
     */
    @Primary
    @Bean(name = "ucRoutingDataSource")
    public UcRoutingDataSource ucRoutingDataSource() throws IOException, SQLException, URISyntaxException {
        // 配置多数据源
        Properties[] arrayProperties = ucDataSourceConfig.getBaseConfigPropetys();
        UcRoutingDataSource ucRoutingDataSource = new UcRoutingDataSource();
        for(Properties configPropety: arrayProperties){
            String dataSourceName = configPropety.getProperty("name");
            Properties properties = ucDataSourceConfig.convertConfigPropety(configPropety);
            DataSource dataSource = ucDataSourceOper.connectDataSource(dataSourceName,properties);
        }
        // 默认数据源
        if (UcDataSourceOper.getMapDataSource().size() > 0) {
            ucRoutingDataSource.setDefaultTargetDataSource(UcDataSourceOper.getMapDataSource().get(UcDataSourceOper.getDefaultDataSourceName()));
        }
        ucRoutingDataSource.setTargetDataSources(UcDataSourceOper.getMapDataSource());
        return ucRoutingDataSource;
    }

    /**
     * 根据数据源创建SqlSessionFactory
     */
    @Bean
    public SqlSessionFactory sqlSessionFactory(@Qualifier(value = "ucRoutingDataSource")UcRoutingDataSource ucRoutingDataSource) throws Exception {
        SqlSessionFactoryBean factoryBean = new SqlSessionFactoryBean();
        factoryBean.setDataSource(ucRoutingDataSource);

        //添加XML目录
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            String sMapperPath = UcDataSourceImport.getDbMapperPath();
            Resource [] resources = resolver.getResources(sMapperPath);
            factoryBean.setMapperLocations(resources);
            factoryBean.getObject().getConfiguration().setMapUnderscoreToCamelCase(true);
            return factoryBean.getObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    /**
     * 配置事务管理器
     */
    @Bean
    public DataSourceTransactionManager transactionManager(@Qualifier(value = "ucRoutingDataSource")UcRoutingDataSource ucRoutingDataSource) throws Exception {
        return new DataSourceTransactionManager(ucRoutingDataSource);
    }

/*
    @Bean
    PageHelperProperties pageHelperProperties(){
        PageHelperProperties  pageHelperProperties = new PageHelperProperties();
        PageInterceptor pageInterceptor = new PageInterceptor();
        pageHelperProperties.setAutoRuntimeDialect("true");
        pageHelperProperties.setCloseConn("true");
        pageHelperProperties.setAutoDialect("true");
        return pageHelperProperties;
    }
*/

}
