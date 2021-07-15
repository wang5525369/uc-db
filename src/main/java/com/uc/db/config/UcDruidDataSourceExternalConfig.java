package com.uc.db.config;

import com.uc.db.annotation.UcEnableDataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.Properties;

/**
 * Created by wangxiaobo on 2018/9/3.
 */
@ConditionalOnBean(annotation = {UcEnableDataSource.class})
@Configuration
@PropertySource(value={"classpath:/uc-druid-datasource-extern-config.properties"},encoding="utf-8",ignoreResourceNotFound=false)
@ConfigurationProperties(prefix="druid")
public class UcDruidDataSourceExternalConfig extends Properties{
}
