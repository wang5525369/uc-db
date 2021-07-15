package com.uc.mongodb.jdbc;

import com.uc.base.jdbc.UcConnection;
import com.uc.mongodb.oper.MongoDbOper;
import lombok.SneakyThrows;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class UcMongoDbDriver implements java.sql.Driver{
    private static final UcMongoDbDriver INSTANCE = new UcMongoDbDriver();
    private static boolean registered;

    static {
        load();
    }

    public static synchronized Driver load() {
        try {
            if (!registered) {
                registered = true;
                DriverManager.registerDriver(INSTANCE);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Can't register driver!");
        }
        return INSTANCE;
    }

    public UcMongoDbDriver(){

    }

    @SneakyThrows
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        info.put("url",url);
        MongoDbOper mongoDbOper = new MongoDbOper();
        UcConnection ucConnection = new UcConnection(info,mongoDbOper);
        return ucConnection;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("不支持getParentLogger");
    }
}
