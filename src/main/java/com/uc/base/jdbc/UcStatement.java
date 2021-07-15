package com.uc.base.jdbc;

import com.google.common.collect.Maps;
import com.uc.base.oper.DbOper;
import com.uc.base.parser.UcSqlParser;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class UcStatement implements Statement, UcJdbcWrapper {
    private boolean closed = false;
    private boolean closeOnCompletion = false;
    private boolean ignoreResultSetClose = false;
    private int queryTimeout = 3000;
    private int fetchSize = 1000;

    protected UcConnection ucConnection = null;
    protected UcSqlParser ucSqlParser = null;
    protected UcResultSet ucResultSet = null;
    protected Map<Integer, UcParamInfo> mapParam = Maps.newHashMap();

    public UcStatement(Connection connection, UcSqlParser ucSqlParser){
        this.ucConnection = (UcConnection) connection;
        this.ucSqlParser = ucSqlParser;
        ucResultSet = new UcResultSet(this,ucConnection, ucSqlParser);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (!this.execute(sql)) {
            throw new SQLException("Invalid sql query [" + sql + "]");
        } else {
            return this.ucResultSet;
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("PreparedStatement禁止使用executeUpdate方法");
    }

    @Override
    public void close() throws SQLException {
        if (!this.closed) {
            this.closed = true;
            this.closeResultSet();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        this.checkOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        this.checkOpen();
        if (max < 0) {
            throw new SQLException("非法的数据");
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        long result = this.getLargeMaxRows();
        if (result > Integer.MAX_VALUE) {
            throw new SQLException("超过最大行数2147483647");
        } else {
            return Math.toIntExact(result);
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.setLargeMaxRows((long)max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        this.checkOpen();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        this.checkOpen();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.checkOpen();
        if (seconds < 0) {
            throw new SQLException("查询超时设置必须大于0");
        } else {
            queryTimeout = seconds;
        }
    }

    @Override
    public void cancel() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持cancel");
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
    public void setCursorName(String name) throws SQLException {
        this.checkOpen();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        getUcResultSet(sql);
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return ucResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        long count = ucResultSet.getUpdateRows();
        return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) (count < Integer.MIN_VALUE ? Integer.MIN_VALUE : count);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        this.checkOpen();
        this.closeResultSet();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.checkOpen();

        if (ResultSet.FETCH_REVERSE != direction || ResultSet.FETCH_FORWARD != direction ||  ResultSet.FETCH_UNKNOWN != direction) {
            throw new SQLException("非法的direction specified");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        this.checkOpen();
        return ResultSet.FETCH_UNKNOWN;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.checkOpen();
        if (rows < 0) {
            throw new SQLException("FetchSize必须大于0");
        } else {
            fetchSize = rows;
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        this.checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        this.checkOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持addBatch");
    }

    @Override
    public void clearBatch() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持clearBatch");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持executeBatch");
    }

    @Override
    public Connection getConnection() throws SQLException {
        this.checkOpen();
        return ucConnection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        this.checkOpen();
        if (1 == current) {
            this.closeResultSet();
            return false;
        } else if (2 != current && 3 != current) {
            throw new SQLFeatureNotSupportedException("不支持getMoreResults");
        } else {
            throw new SQLException("非法参数");
        }
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持getGeneratedKeys");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持executeUpdate");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持executeUpdate");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        this.checkOpen();
        throw new SQLFeatureNotSupportedException("不支持executeUpdate");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        this.checkOpen();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.checkOpen();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        this.checkOpen();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.checkOpen();
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        this.checkOpen();
        return this.closeOnCompletion;
    }

    protected final void closeResultSet() throws SQLException {
        if (this.ucResultSet != null) {
            this.ignoreResultSetClose = true;

            try {
                this.ucResultSet.close();
            } finally {
                this.ucResultSet = null;
                this.ignoreResultSetClose = false;
            }
        }
    }

    protected final void checkOpen() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Statement is closed");
        }
    }

    protected final void getUcResultSet(String sql) throws SQLException {
        if (StringUtils.isEmpty(sql) == false){
            ucSqlParser.parserSql(sql);
        }
        DbOper dbOper = ucConnection.getDbOper();
        List<UcColumnInfo> listUcColumnInfo = dbOper.getColumnInfo(ucSqlParser,mapParam);

        List<Object> listObject = dbOper.execute(ucSqlParser,mapParam);
        ucResultSet.convertResultSet(listUcColumnInfo,listObject);
    }
}
