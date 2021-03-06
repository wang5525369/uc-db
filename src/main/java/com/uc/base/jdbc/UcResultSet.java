package com.uc.base.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uc.base.parser.UcSqlParser;
import com.uc.base.sql.UcType;
import com.uc.base.sql.UcTypeConverter;
import com.uc.base.utils.UcJdbcDateUtils;
import org.springframework.util.CollectionUtils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.function.Function;

public class UcResultSet implements ResultSet, UcJdbcWrapper {
    private final Calendar defaultCalendar = Calendar.getInstance(Locale.ROOT);
    private Map<String, UcColumnInfo> mapColumnNameToIndex = new LinkedHashMap();
    private Map<Integer,UcColumnInfo> mapIndexToColumnName = new LinkedHashMap();
    private UcResultSetMetaData ucResultSetMetaData = null;
    private UcSqlParser ucSqlParser = null;
    private UcConnection ucConnection = null;
    private UcStatement ucStatement = null;
    private Map<Integer, Object> mapRowContent = Maps.newHashMap();
    private List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
    private List<Object> listRowContent = Lists.newArrayList();
    private int currentRow = -1;
    private int totalRows = 0;
    private int totalColumns = 0;
    private boolean closed = false;
    private boolean wasNull = false;

    public UcResultSet(UcConnection ucConnection){
        this.ucConnection = ucConnection;
    }

    public UcResultSet(UcStatement ucStatement, UcConnection ucConnection, UcSqlParser ucSqlParser){
        this.ucStatement = ucStatement;
        this.ucConnection = ucConnection;
        this.ucSqlParser = ucSqlParser;
    }

    private void init(){
        currentRow = -1;
        int totalRows = 0;
        totalColumns = 0;
        this.listRowContent.clear();
        this.listUcColumnInfo .clear();
        this.mapColumnNameToIndex.clear();
        this.mapIndexToColumnName.clear();
        this.mapRowContent.clear();
    }

    public void convertResultSet(List<UcColumnInfo> listUcColumnInfo,List<Object> listObject) throws SQLException {
        init();
        this.listRowContent = listObject;
        this.listUcColumnInfo = listUcColumnInfo;
        for(UcColumnInfo ucColumnInfo : listUcColumnInfo){
            mapColumnNameToIndex.put(ucColumnInfo.name,ucColumnInfo);
            mapIndexToColumnName.put(ucColumnInfo.index,ucColumnInfo);
        }
        totalColumns = listUcColumnInfo.size();
        ucResultSetMetaData = new UcResultSetMetaData(this,listUcColumnInfo);
        totalRows = listRowContent.size();
    }

    void checkOpen() throws SQLException {
        if (this.isClosed() == true) {
            throw new SQLException("??????????????????");
        }
    }

    @Override
    public boolean next() throws SQLException {
        this.checkOpen();
        currentRow++;
        if (currentRow<totalRows) {
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (!this.closed == false) {
            this.closed = true;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        this.checkOpen();
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return (String)this.getObject(columnIndex, String.class);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return this.column(columnIndex) != null ? (Boolean)this.getObject(columnIndex, Boolean.class) : false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return this.column(columnIndex) != null ? (Byte)this.getObject(columnIndex, Byte.class) : 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return this.column(columnIndex) != null ? (Short)this.getObject(columnIndex, Short.class) : 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return this.column(columnIndex) != null ? (Integer)this.getObject(columnIndex, Integer.class) : 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return this.column(columnIndex) != null ? (Long)this.getObject(columnIndex, Long.class) : 0L;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return this.column(columnIndex) != null ? (Float)this.getObject(columnIndex, Float.class) : 0.0F;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return this.column(columnIndex) != null ? (Double)this.getObject(columnIndex, Double.class) : 0.0D;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????BigDecimal");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            return (byte[])this.column(columnIndex);
        } catch (ClassCastException ex) {
            throw new SQLException("???????????????" + columnIndex + " to a byte array", ex);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return this.getDate(columnIndex, (Calendar)null);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return this.getTime(columnIndex, (Calendar)null);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return this.getTimestamp(columnIndex, (Calendar)null);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????AsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????UnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????BinaryStream");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return this.getString(this.column(columnLabel).index);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return this.getBoolean(this.column(columnLabel).index);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return this.getByte(this.column(columnLabel).index);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return this.getShort(this.column(columnLabel).index);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return this.getInt(this.column(columnLabel).index);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return this.getLong(this.column(columnLabel).index);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return this.getFloat(this.column(columnLabel).index);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return this.getDouble(this.column(columnLabel).index);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????BigDecimal");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return this.getBytes(this.column(columnLabel).index);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return this.getDate(this.column(columnLabel).index);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return this.getTime(this.column(columnLabel).index);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return this.getTimestamp(this.column(columnLabel).index);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????AsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????UnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????BinaryStream");
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
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????CursorName");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return ucResultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return this.convert(columnIndex, (Class)null);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return this.getObject(this.column(columnLabel).index);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return this.column(columnLabel).index;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????CharacterStream");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????CharacterStream");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????BigDecimal");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????BigDecimal");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentRow == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????isAfterLast");
    }

    @Override
    public boolean isFirst() throws SQLException {
        return currentRow == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????isLast");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException("?????????beforeFirst");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLException("?????????afterLast");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException("?????????first");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException("?????????last");
    }

    @Override
    public int getRow() throws SQLException {
        return currentRow;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLException("?????????absolute");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLException("?????????relative");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException("?????????previous");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("????????? FETCH_FORWARD");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        this.checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.checkOpen();
        if (rows < 0) {
            throw new SQLException("??????????????????0");
        } else if (rows != this.getFetchSize()) {
            throw new SQLException("setFetchSize??????");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        this.checkOpen();
        return totalRows;
    }

    @Override
    public int getType() throws SQLException {
        //????????????
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????rowUpdated");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????rowInserted");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????rowDeleted");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNull");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateByte");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateShort");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateLong");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateFloat");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateDouble");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBigDecimal");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateString");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBytes");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateDate");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateObject");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNull");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBoolean");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateByte");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateShort");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateInt");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateLong");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateFloat");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateDouble");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBigDecimal");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateString");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBytes");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateDate");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateTime");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateTimestamp");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateCharacterStream");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateObject");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????insertRow");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateRow");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????deleteRow");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????refreshRow");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????moveToCurrentRow");
    }

    @Override
    public Statement getStatement() throws SQLException {
        return ucStatement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) {
            throw new SQLFeatureNotSupportedException("?????????map??????");
        } else {
            return this.getObject(columnIndex);
        }
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????Ref");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????Blob");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????Clob");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????Array");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return this.getObject(this.column(columnLabel).index, map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????Ref");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????Blob");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????Clob");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????Array");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return UcTypeConverter.convertDate(this.dateTime(columnIndex), this.safeCalendar(cal));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return this.getDate(this.column(columnLabel).index, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return UcTypeConverter.convertTime(this.dateTime(columnIndex), this.safeCalendar(cal));
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return this.getTime(this.column(columnLabel).index, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return UcTypeConverter.convertTimestamp(this.dateTime(columnIndex), this.safeCalendar(cal));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return this.getTimestamp(this.column(columnLabel).index, cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????URL");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????URL");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateRef");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateRef");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateClob");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateArray");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateArray");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????RowId");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????RowId");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateRowId");
    }

    @Override
    public int getHoldability() throws SQLException {
        this.checkOpen();
        return 1;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNClob");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????NClob");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????NClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????SQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????SQLXML");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateSQLXML");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????NString");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????NString");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????NCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????NCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNClob");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("?????????????????????updateNClob");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("??????????????????");
        } else {
            return this.convert(columnIndex, type);
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("????????????");
        } else {
            return this.convert(columnLabel, type);
        }
    }

    private Object getRowObject(int nRow){
        Object object = mapRowContent.get(nRow);
        if (object != null) {
            return object;
        }else{
            object = listRowContent.get(currentRow);
            mapRowContent.put(currentRow, object);
        }
        return object;
    }

    private <T> T convert(String columnLabel,Class<T> type) throws SQLException {
        Object o = getColumnObjectByColumnName(columnLabel);
        if (o == null) {
            return (T) o;
        }
        UcColumnInfo ucColumnInfo = getUcColumnInfo(columnLabel);
        UcType ucType = ucColumnInfo.type;
        String typeString = type != null ? type.getSimpleName() : ucType.getName();
        return UcTypeConverter.convert(o, ucType, type, typeString);
    }

    private <T> T convert(int columnIndex, Class<T> type) throws SQLException {
        UcColumnInfo ucColumnInfo = mapIndexToColumnName.get(columnIndex);
        String columnLabel = ucColumnInfo.name;
        if (columnLabel == null) {
            throw new SQLException("?????????????????? [" + columnIndex + "]");
        } else {
            return convert(columnLabel,type);
        }
    }

    private Object column(int columnIndex) throws SQLException {
        this.checkOpen();
        if (columnIndex >= 1 && columnIndex <= totalColumns) {
            Object object = null;

            try {
                object = getColumnObjectByColumnIndex(columnIndex);
            } catch (IllegalArgumentException ex) {
                throw new SQLException(ex.getMessage());
            }

            this.wasNull = object == null;
            return object;
        } else {
            throw new SQLException("?????????????????? [" + columnIndex + "]");
        }
    }

    private UcColumnInfo column(String columnName) throws SQLException {
        this.checkOpen();
        UcColumnInfo ucColumnInfo = this.mapColumnNameToIndex.get(columnName);
        if (ucColumnInfo == null) {
            throw new SQLException("???????????????[" + columnName + "]");
        } else {
            return ucColumnInfo;
        }
    }

    Object getColumnObjectByColumnName(String columnName) throws SQLException {
        Object object = getRowObject(currentRow);
        if (object == null){
            return null;
        }
        Object  o = ucConnection.getDbOper().getColumnValue(object,columnName);
        if (o == null){
            return null;
        }
        return o;
    }

    Object getColumnObjectByColumnIndex(int columnIndex) throws SQLException {
        UcColumnInfo ucColumnInfo = mapIndexToColumnName.get(columnIndex);
        String columnName = ucColumnInfo.name;
        if (columnName == null){
            throw new SQLException("?????????????????? [" + columnIndex + "]");
        }
        Object o = getColumnObjectByColumnName(columnName);
        return o;
    }

    UcColumnInfo getUcColumnInfo(String columnName) throws SQLException {
        UcColumnInfo ucColumnInfo = mapColumnNameToIndex.get(columnName);
        if (ucColumnInfo == null){
            throw new SQLException("?????????????????? [" + columnName + "]");
        }
        return ucColumnInfo;
    }

    UcColumnInfo getUcColumnInfo(int columnIndex) throws SQLException {
        UcColumnInfo ucColumnInfo = listUcColumnInfo.get(columnIndex);
        if (ucColumnInfo == null){
            throw new SQLException("?????????????????? [" + columnIndex + "]");
        }
        return ucColumnInfo;
    }

    private Long dateTime(int columnIndex) throws SQLException {
        Object o = this.column(columnIndex);
        UcColumnInfo ucColumnInfo = getUcColumnInfo(columnIndex);
        UcType type = ucColumnInfo.type;

        try {
            if (UcType.DATE == type) {
                return o == null ? null : (Long) UcJdbcDateUtils.asDateTimeField(o, UcJdbcDateUtils::asMillisSinceEpoch, Function.identity());
            } else {
                return o == null ? null : (Long)o;
            }
        } catch (ClassCastException var5) {
            throw new SQLException(String.format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Long", o, type.getName()), var5);
        }
    }

    private Calendar safeCalendar(Calendar calendar) {
        return calendar == null ? this.defaultCalendar : calendar;
    }

    public int getUpdateRows() throws SQLException {
        int update = 0;
        if (CollectionUtils.isEmpty(listRowContent) == false){
            if (next() == true) {
                update = (int) getColumnObjectByColumnIndex(1);
            }
        }
        return update;
    }
}
