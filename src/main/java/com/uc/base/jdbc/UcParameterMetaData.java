package com.uc.base.jdbc;

import com.uc.base.utils.UcTypeUtils;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class UcParameterMetaData implements ParameterMetaData,UcJdbcWrapper {
    private UcPreparedStatement ucPreparedStatement = null;

    public UcParameterMetaData(UcPreparedStatement ucPreparedStatement){
        this.ucPreparedStatement = ucPreparedStatement;
    }

    @Override
    public int getParameterCount() throws SQLException {
        this.ucPreparedStatement.checkOpen();
        return this.ucPreparedStatement.mapParam.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        this.ucPreparedStatement.checkOpen();
        return 2;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return UcTypeUtils.isSigned(this.paramInfo(param).getType());
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        this.ucPreparedStatement.checkOpen();
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        this.ucPreparedStatement.checkOpen();
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return this.paramInfo(param).getType().getVendorTypeNumber();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return this.paramInfo(param).getType().getName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return UcTypeUtils.classOf(this.paramInfo(param).getType()).getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        this.ucPreparedStatement.checkOpen();
        return 0;
    }

    private UcParamInfo paramInfo(int param) throws SQLException {
        this.ucPreparedStatement.checkOpen();
        return this.ucPreparedStatement.mapParam.get(param);
    }
}
