package com.uc.db.datasource;

import com.uc.db.dbholder.UcDataSourceContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * Created by wangxiaobo on 2018/1/19.
 */
public class UcRoutingDataSource extends AbstractRoutingDataSource {
    private static final Logger logger = LoggerFactory.getLogger(UcRoutingDataSource.class);

    @Override
    protected Object determineCurrentLookupKey() {
        return UcDataSourceContextHolder.getDataSourceName();
    }
}
