package com.uc.db.dbholder;

import com.uc.db.annotation.UcEnableDataSource;
import com.uc.db.datasource.UcDataSourceOper;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;


/**
 * Created by wangxiaobo on 2018/1/19.
 */
@ConditionalOnBean(annotation = {UcEnableDataSource.class})
@Aspect
@Order(-1)// 保证该AOP在@Transactional之前执行
@Component
public class UcDataSourceContextHolder {
    public static final Logger logger = LoggerFactory.getLogger(UcDataSourceContextHolder.class);

    private static final ThreadLocal<String> contextHolder = new ThreadLocal<>();

    // 设置数据源名
    public static void setDataSourceName(String dataSourceName) {
        if (UcDataSourceOper.getMapDataSourceProperties().containsKey(dataSourceName) == false){
            logger.debug("数据源{}不存在,设置为默认数据源{}", dataSourceName, UcDataSourceOper.getDefaultDataSourceName());
        }else {
            logger.debug("设置数据源为{}", dataSourceName);
        }
        contextHolder.set(dataSourceName);
    }

    // 获取数据源名
    public static String getDataSourceName() {
        return (contextHolder.get())==null?"":(contextHolder.get());
    }

    // 清除数据源名
    public static void clearDataSourceName() {
        contextHolder.remove();
    }
}
