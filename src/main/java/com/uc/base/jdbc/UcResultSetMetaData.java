package com.uc.base.jdbc;

import com.google.common.collect.Lists;
import com.uc.base.sql.UcType;
import com.uc.base.utils.UcTypeUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

public class UcResultSetMetaData implements ResultSetMetaData, UcJdbcWrapper {

    private UcResultSet ucResultSet = null;
    private List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();

    public UcResultSetMetaData(UcResultSet ucResultSet, List<UcColumnInfo> listUcColumnInfo){
        this.ucResultSet = ucResultSet;
        this.listUcColumnInfo = listUcColumnInfo;
    }

    public int getColumnCount() throws SQLException {
        this.checkOpen();
        return this.listUcColumnInfo.size();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        this.column(column);
        return false;
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        this.column(column);
        return true;
    }

    public boolean isSearchable(int column) throws SQLException {
        this.column(column);
        return true;
    }

    public boolean isCurrency(int column) throws SQLException {
        this.column(column);
        return false;
    }

    public int isNullable(int column) throws SQLException {
        this.column(column);
        return 2;
    }

    public boolean isSigned(int column) throws SQLException {
        return UcTypeUtils.isSigned(this.column(column).type);
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        return this.column(column).displaySize();
    }

    public String getColumnLabel(int column) throws SQLException {
        UcColumnInfo info = this.column(column);
        return "".equals(info.label) ? info.name : info.label;
    }

    public String getColumnName(int column) throws SQLException {
        return this.column(column).name;
    }

    public String getSchemaName(int column) throws SQLException {
        return this.column(column).schema;
    }

    public int getPrecision(int column) throws SQLException {
        this.column(column);
        return 0;
    }

    public int getScale(int column) throws SQLException {
        this.column(column);
        return 0;
    }

    public String getTableName(int column) throws SQLException {
        return this.column(column).table;
    }

    public String getCatalogName(int column) throws SQLException {
        return this.column(column).catalog;
    }

    public int getColumnType(int column) throws SQLException {
        return this.column(column).type.getVendorTypeNumber();
    }

    public String getColumnTypeName(int column) throws SQLException {
        return this.column(column).type.getName();
    }

    public boolean isReadOnly(int column) throws SQLException {
        this.column(column);
        return true;
    }

    public boolean isWritable(int column) throws SQLException {
        this.column(column);
        return false;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        this.column(column);
        return false;
    }

    public String getColumnClassName(int column) throws SQLException {
        UcColumnInfo ucColumnInfo = this.column(column);
        UcType ucType = ucColumnInfo.type;
        Class clazz = UcTypeUtils.classOf(ucType);
        return clazz.getName();
    }

    private void checkOpen() throws SQLException {
        return;

    }

    private UcColumnInfo column(int column) throws SQLException {
        this.checkOpen();
        if (column >= 1 && column <= this.listUcColumnInfo.size()) {
            return (UcColumnInfo)this.listUcColumnInfo.get(column - 1);
        } else {
            throw new SQLException("Invalid column index [" + column + "]");
        }
    }

    public String toString() {
        return String.format(Locale.ROOT, "%s(%s)", this.getClass().getSimpleName(), this.listUcColumnInfo);
    }
}
