package com.uc.base.jdbc;

import com.uc.base.parser.UcSqlParser;
import com.uc.base.sql.UcType;
import com.uc.base.sql.UcTypeConverter;
import com.uc.base.utils.UcTypeUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;

public class UcPreparedStatement extends UcStatement implements PreparedStatement {

    public UcPreparedStatement(Connection connection, UcSqlParser ucSqlParser) {
        super(connection, ucSqlParser);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        getUcResultSet("");
        return ucResultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持写操作");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.setParam(parameterIndex, (Object)null, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.setObject(parameterIndex, x, Types.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.setObject(parameterIndex, x, Types.TINYINT);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.setObject(parameterIndex, x, Types.SMALLINT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.setObject(parameterIndex, x, Types.INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.setObject(parameterIndex, x, Types.BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.setObject(parameterIndex, x, Types.REAL);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.setObject(parameterIndex, x, Types.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.setObject(parameterIndex, x, Types.BIGINT);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.setObject(parameterIndex, x, Types.VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.setObject(parameterIndex, x, Types.VARBINARY);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.setObject(parameterIndex, x, Types.TIMESTAMP);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.setObject(parameterIndex, x, Types.TIMESTAMP);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.setObject(parameterIndex, x, Types.TIMESTAMP);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持AsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持UnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持BinaryStream");
    }

    @Override
    public void clearParameters() throws SQLException {
        mapParam.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            this.setParam(parameterIndex, (Object)null, UcType.NULL);
        } else {
            this.checkKnownUnsupportedTypes(x);
            this.setObject(parameterIndex, x, UcTypeUtils.of(x.getClass()).getVendorTypeNumber(), 0);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        this.setObject(parameterIndex, x, UcTypeUtils.of(targetSqlType), targetSqlType.getName());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.setObject(parameterIndex, x, UcTypeUtils.asSqlType(targetSqlType), scaleOrLength);
    }


    @Override
    public boolean execute() throws SQLException {
        this.executeQuery();
        return true;
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持addBatch");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持CharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.ucResultSet != null ? this.ucResultSet.getMetaData() : null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (cal == null) {
            this.setObject(parameterIndex, x, Types.TIMESTAMP);
        } else if (x == null) {
            this.setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            this.setObject(parameterIndex, new Date(UcTypeConverter.convertFromCalendarToUTC(x.getTime(), cal)), 93);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (cal == null) {
            this.setObject(parameterIndex, x, Types.TIMESTAMP);
        } else if (x == null) {
            this.setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            this.setObject(parameterIndex, new Time(UcTypeConverter.convertFromCalendarToUTC(x.getTime(), cal)), 93);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (cal == null) {
            this.setObject(parameterIndex, x, Types.TIMESTAMP);
        } else if (x == null) {
            this.setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            this.setObject(parameterIndex, new Timestamp(UcTypeConverter.convertFromCalendarToUTC(x.getTime(), cal)), 93);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new UcParameterMetaData(this);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持NString");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持NCharacterStream");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.setObject(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持Clob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持Blob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持NClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.setObject(parameterIndex, xmlObject);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持AsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持BinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持CharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持AsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持BinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持CharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持NCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持Clob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持Blob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("不支持NClob");
    }

    private void setParam(int parameterIndex, Object o, int sqlType) throws SQLException {
        this.setParam(parameterIndex, o, UcTypeUtils.of(sqlType));
    }

    private void setParam(int parameterIndex, Object o, UcType type) throws SQLException {
        this.checkOpen();
        UcParamInfo ucParamInfo = new UcParamInfo(o,type,0);
        mapParam.put(parameterIndex,ucParamInfo);
    }

    private void checkKnownUnsupportedTypes(Object o) throws SQLFeatureNotSupportedException {
        List<Class<?>> unsupportedTypes = new ArrayList(Arrays.asList(Struct.class, Array.class, SQLXML.class, RowId.class, Ref.class, Blob.class, NClob.class, Clob.class, LocalDate.class, LocalTime.class, OffsetTime.class, OffsetDateTime.class, URL.class, BigDecimal.class));
        Iterator iterator = unsupportedTypes.iterator();

        Class clazz;
        do {
            if (!iterator.hasNext()) {
                return;
            }

            clazz = (Class)iterator.next();
        } while(!clazz.isAssignableFrom(o.getClass()));

        throw new SQLFeatureNotSupportedException("Objects of type [" + clazz.getName() + "] are not supported");
    }

    private void setObject(int parameterIndex, Object x, UcType dataType, String typeString) throws SQLException {
        this.checkOpen();
        if (x == null) {
            this.setParam(parameterIndex, (Object)null, dataType);
        } else {
            this.checkKnownUnsupportedTypes(x);
            if (x instanceof ObjectId || x instanceof Document || x instanceof ArrayList){
                this.setParam(parameterIndex, UcTypeConverter.convert(x, UcTypeUtils.of(x.getClass()), UcTypeUtils.classOf(dataType), typeString), dataType);
            }else if (x instanceof byte[]) {
                if (dataType != UcType.BINARY) {
                    throw new SQLFeatureNotSupportedException("Conversion from type [byte[]] to [" + typeString + "] not supported");
                } else {
                    this.setParam(parameterIndex, x, UcType.BINARY);
                }
            } else if (!(x instanceof Timestamp) && !(x instanceof Calendar) && !(x instanceof Date) && !(x instanceof LocalDateTime) && !(x instanceof Time) && !(x instanceof java.util.Date)) {
                if (!(x instanceof Boolean) && !(x instanceof Byte) && !(x instanceof Short) && !(x instanceof Integer) && !(x instanceof Long) && !(x instanceof Float) && !(x instanceof Double) && !(x instanceof String)) {
                    throw new SQLFeatureNotSupportedException("Conversion from type [" + x.getClass().getName() + "] to [" + typeString + "] not supported");
                } else {
                    this.setParam(parameterIndex, UcTypeConverter.convert(x, UcTypeUtils.of(x.getClass()), UcTypeUtils.classOf(dataType), typeString), dataType);
                }
            } else if (dataType == UcType.DATE) {
                java.util.Date dateToSet;
                if (x instanceof Timestamp) {
                    dateToSet = new java.util.Date(((Timestamp)x).getTime());
                } else if (x instanceof Calendar) {
                    dateToSet = ((Calendar)x).getTime();
                } else if (x instanceof Date) {
                    dateToSet = new java.util.Date(((Date)x).getTime());
                } else if (x instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime)x;
                    Calendar cal = this.getDefaultCalendar();
                    cal.set(ldt.getYear(), ldt.getMonthValue() - 1, ldt.getDayOfMonth(), ldt.getHour(), ldt.getMinute(), ldt.getSecond());
                    dateToSet = cal.getTime();
                } else if (x instanceof Time) {
                    dateToSet = new java.util.Date(((Time)x).getTime());
                } else {
                    dateToSet = (java.util.Date)x;
                }

                this.setParam(parameterIndex, dateToSet, dataType);
            } else if (UcTypeUtils.isString(dataType)) {
                this.setParam(parameterIndex, String.valueOf(x), dataType);
            } else {
                throw new SQLFeatureNotSupportedException("Conversion from type [" + x.getClass().getName() + "] to [" + typeString + "] not supported");
            }
        }
    }

    private Calendar getDefaultCalendar() {
        return Calendar.getInstance(Locale.ROOT);
    }

}
