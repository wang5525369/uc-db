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
 * Created by wangxiaobo on 2018/9/3.
 */
public class UcSingleDataSource {
    private static final Logger logger = LoggerFactory.getLogger(UcSingleDataSource.class);

    @Autowired
    UcDataSourceConfig ucDataSourceConfig;

    UcDataSourceOper ucDataSourceOper = new UcDataSourceOper();

    @Bean
    DataSource ucDataSource() throws SQLException, IOException, URISyntaxException {
        Properties configPropety = ucDataSourceConfig.getBaseConfigPropetys()[0];
        String dataSourceName = configPropety.getProperty("name");
        configPropety = ucDataSourceConfig.convertConfigPropety(configPropety);
        DataSource dataSource = ucDataSourceOper.connectDataSource(dataSourceName,configPropety);
        return dataSource;
    }

    @Bean
    public SqlSessionFactory sqlSessionFactory(@Qualifier(value = "ucDataSource") DataSource ucDataSource) throws Exception {
        SqlSessionFactoryBean factoryBean = new SqlSessionFactoryBean();
        factoryBean.setDataSource(ucDataSource);

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
    public DataSourceTransactionManager transactionManager(@Qualifier(value = "ucDataSource") DataSource ucDataSource) throws Exception {
        return new DataSourceTransactionManager(ucDataSource);
    }
}

