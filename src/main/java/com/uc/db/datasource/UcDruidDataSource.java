package com.uc.db.datasource;


import com.alibaba.druid.pool.DruidDataSource;
import com.uc.db.config.UcDataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

public class UcDruidDataSource implements Closeable, DataSource, ConnectionPoolDataSource {

    private static final Logger logger = LoggerFactory.getLogger(UcDruidDataSource.class);

    private volatile boolean inited = false;

    private volatile DruidDataSource currentDruidDataSource = null;

    public synchronized void init(Properties properties) throws SQLException, IOException {
        if (inited == false) {
            currentDruidDataSource = buildDruidDataSource(properties);
        }else{
            reload(properties);
        }

    }

    private void reload(Properties properties) throws IOException, SQLException {
        logger.info("重启数据源开始");
        final DruidDataSource newDruidDataSource = buildDruidDataSource(properties);
        final DruidDataSource oldDruidDataSource = currentDruidDataSource;
        currentDruidDataSource = newDruidDataSource;
        try {
            oldDruidDataSource.close();
        } catch (Exception e) {
            logger.error("重启数据源后关闭老数据源失败", e);
        }
        logger.info("重启数据源结束");
    }

    private DruidDataSource buildDruidDataSource(Properties properties) throws IOException, SQLException {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.configFromPropety(properties);
        Integer nMaxWait = Integer.parseInt(properties.getProperty("druid.maxWait"));
        if (nMaxWait == null)
            nMaxWait = 0;
        druidDataSource.setMaxWait(nMaxWait);
        Integer nQueryTimeout = Integer.parseInt(properties.getProperty("druid.queryTimeout"));
        if (nQueryTimeout == null)
            nQueryTimeout = 0;
        druidDataSource.setQueryTimeout(nQueryTimeout);
        Integer nTransactionQueryTimeout = Integer.parseInt(properties.getProperty("druid.transactionQueryTimeout"));
        if (nTransactionQueryTimeout == null)
            nTransactionQueryTimeout = 0;
        druidDataSource.setTransactionQueryTimeout(nTransactionQueryTimeout);
        Properties connectProperties = UcDataSourceConfig.buildDruidProperties(properties.getProperty("druid.connectProperties"), ";");
        druidDataSource.setConnectProperties(connectProperties);
        druidDataSource.init();
        inited = true;
        return druidDataSource;
    }

    @Override
    public void close() throws IOException {
        currentDruidDataSource.close();
    }

    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        final PooledConnection pooledConnection = currentDruidDataSource.getPooledConnection();
        return pooledConnection;
    }

    @Override
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        final PooledConnection pooledConnection = currentDruidDataSource.getPooledConnection(user, password);
        return pooledConnection;
    }

    @Override
    public Connection getConnection() throws SQLException {
        final Connection connection = currentDruidDataSource.getConnection();
        return connection;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        final Connection connection = currentDruidDataSource.getConnection(username, password);
        return connection;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return currentDruidDataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return currentDruidDataSource.isWrapperFor(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return currentDruidDataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        currentDruidDataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        currentDruidDataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return currentDruidDataSource.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return currentDruidDataSource.getParentLogger();
    }
}
