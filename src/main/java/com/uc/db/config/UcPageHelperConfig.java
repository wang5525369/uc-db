package com.uc.db.config;

import com.uc.db.datasource.UcMultiDataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Created by wangxiaobo on 2019/8/14.
 */
@ConditionalOnClass(value = {UcMultiDataSource.class})
@Configuration
@PropertySource(value={"classpath:/uc-pagehelper.properties"},encoding="utf-8",ignoreResourceNotFound=false)
@ConfigurationProperties(prefix="pagehelper")
public class UcPageHelperConfig {
    String autoDialect;
    String closeConn;
    String autoRuntimeDialect;

    public String getAutoDialect() {
        return autoDialect;
    }

    public void setAutoDialect(String autoDialect) {
        this.autoDialect = autoDialect;
    }

    public String getCloseConn() {
        return closeConn;
    }

    public void setCloseConn(String closeConn) {
        this.closeConn = closeConn;
    }

    public String getAutoRuntimeDialect() {
        return autoRuntimeDialect;
    }

    public void setAutoRuntimeDialect(String autoRuntimeDialect) {
        this.autoRuntimeDialect = autoRuntimeDialect;
    }
}
