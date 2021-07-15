package com.uc.base.jdbc;

import com.uc.base.oper.DbOper;
import com.uc.base.parser.UcSqlParser;
import lombok.SneakyThrows;

import java.net.URISyntaxException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class UcConnection implements UcJdbcWrapper, Connection {
    private DbOper dbOper;
    private boolean closed = true;
    private String catalog;
    private String schema;
    private String url;
    private String userName;

    public UcConnection(Properties properties,DbOper dbOper) throws SQLException, URISyntaxException {
        this.dbOper = dbOper;
        init(properties);
    }

    void init(Properties properties) throws SQLException, URISyntaxException {
        url = properties.getProperty("url");
        closed = dbOper.init(properties);
    }


    @Override
    public Statement createStatement() throws SQLException {
        this.checkOpen();
        UcSqlParser ucSqlParser = new UcSqlParser();
        return new UcStatement(this, ucSqlParser);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        boolean bParser = false;
        UcSqlParser ucSqlParser = new UcSqlParser();
        bParser = ucSqlParser.parserSql(sql);
        if (bParser == false){
            String reason = ucSqlParser.getErrorMsg();
            throw  new SQLException(reason);
        }

        UcPreparedStatement ucPreparedStatement = new UcPreparedStatement(this, ucSqlParser);

        return ucPreparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持prepareCall");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        this.checkOpen();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.checkOpen();
        if (!autoCommit) {
            new SQLFeatureNotSupportedException("不支持setAutoCommit");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        this.checkOpen();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        this.checkOpen();
        if (this.getAutoCommit()) {
            throw new SQLException("默认已提交");
        } else {
            throw new SQLFeatureNotSupportedException("不支持commit");
        }
    }

    @Override
    public void rollback() throws SQLException {
        this.checkOpen();
        if (this.getAutoCommit()) {
            throw new SQLException("默认已回滚");
        } else {
            throw new SQLFeatureNotSupportedException("不支持rollback");
        }
    }

    @Override
    public boolean isClosed(){
        return this.closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new UcDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!readOnly) {
            throw new SQLFeatureNotSupportedException("不支持setReadOnly");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        this.checkOpen();
        return true;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.checkOpen();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.checkOpen();
        if (0 != level) {
            throw new SQLFeatureNotSupportedException("不支持setTransactionIsolation");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        this.checkOpen();
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        this.checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.checkOpen();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        this.checkResultSet(resultSetType, resultSetConcurrency);
        return this.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        this.checkResultSet(resultSetType, resultSetConcurrency);
        return this.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        this.checkResultSet(resultSetType, resultSetConcurrency);
        return this.prepareCall(sql);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持getTypeMap");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持setTypeMap");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        this.checkOpen();
        this.checkHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        this.checkOpen();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持setSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持rollback");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持releaseSavepoint");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        this.checkOpen();
        this.checkHoldability(resultSetHoldability);
        return this.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        this.checkOpen();
        this.checkHoldability(resultSetHoldability);
        return this.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        this.checkOpen();
        this.checkHoldability(resultSetHoldability);
        return this.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        this.checkOpen();
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException("不支持prepareStatement");
        } else {
            return this.prepareStatement(sql);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持PreparedStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持PreparedStatement");
    }

    @Override
    public Clob createClob() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持createClob");
    }

    @Override
    public Blob createBlob() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持createClob");
    }

    @Override
    public NClob createNClob() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持createNClob");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持createSQLXML");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Negative timeout");
        } else {
            return !this.isClosed();
        }
    }

    @SneakyThrows
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        this.checkOpenClientInfo();
        throw new SQLClientInfoException("不支持setClientInfo", (Map)null);
    }

    @SneakyThrows
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.checkOpenClientInfo();
        throw new SQLClientInfoException("不支持setClientInfo", (Map)null);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        this.checkOpenClientInfo();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        this.checkOpenClientInfo();
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持createStruct");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.checkOpen();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return this.schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持abort");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持setNetworkTimeout");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持getNetworkTimeout");
    }



    @Override
    public void close() throws SQLException {
        return;
    }

    private void checkOpen() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    private void checkResultSet(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (ResultSet.TYPE_FORWARD_ONLY != resultSetType) {
            throw new SQLFeatureNotSupportedException("只支持TYPE_FORWARD_ONLY");
        } else if (ResultSet.CONCUR_READ_ONLY != resultSetConcurrency) {
            throw new SQLFeatureNotSupportedException("只支持CONCUR_READ_ONLY");
        }
    }

    private void checkHoldability(int resultSetHoldability) throws SQLException {
        if (ResultSet.HOLD_CURSORS_OVER_COMMIT != resultSetHoldability) {
            throw new SQLFeatureNotSupportedException("只支持HOLD_CURSORS_OVER_COMMIT");
        }
    }

    private void checkOpenClientInfo() throws SQLException {
        if (this.isClosed()) {
            throw new SQLClientInfoException("连接已关闭", (Map)null);
        }
    }

    public void realClose(){
        if (!this.isClosed()) {
            dbOper.close();
            this.closed = true;
        }
    }

    public String getUrl() {
        return url;
    }

    public String getUserName() {
        return userName;
    }

    public DbOper getDbOper() {
        return dbOper;
    }
}
