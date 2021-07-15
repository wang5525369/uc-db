package com.uc.es.jdbc;

import com.uc.base.jdbc.UcPoolConnection;
import com.uc.base.jdbc.UcJdbcWrapper;
import com.uc.es.oper.EsOper;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class UcEsDataSource implements DataSource, UcJdbcWrapper, Closeable {

    private PrintWriter writer;
    private int loginTimeout;
    private Properties props;
    private EsOper esOper = new EsOper();
    private UcPoolConnection ucPoolConnection = new UcPoolConnection();

    public synchronized void init(Properties properties) throws UnknownHostException, SQLException, URISyntaxException {
        props = properties;
        ucPoolConnection.init(properties,esOper);
    }

    @SneakyThrows
    @Override
    public Connection getConnection(){
        return ucPoolConnection.getConnection();
    }

    @SneakyThrows
    @Override
    public Connection getConnection(String username, String password){
        return ucPoolConnection.getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return writer;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.writer = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return this.loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("不支持getParentLogger");
    }

    @Override
    public void close(){
        ucPoolConnection.closeAllConnection();
    }
}
