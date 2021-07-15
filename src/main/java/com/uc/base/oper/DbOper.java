package com.uc.base.oper;

import com.uc.base.jdbc.UcColumnInfo;
import com.uc.base.jdbc.UcParamInfo;
import com.uc.base.jdbc.UcTableInfo;
import com.uc.base.parser.UcSqlParser;
import org.apache.commons.lang3.tuple.Pair;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface DbOper {
    List<UcTableInfo> getTables() throws SQLException;
    Pair<String,List<UcColumnInfo>> getColumnInfo(String tableName) throws SQLException;
    List<UcColumnInfo> getColumnInfo(UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException;
    List<Object> execute(UcSqlParser ucSqlParser, Map<Integer, UcParamInfo> mapParam) throws SQLException;
    Object getColumnValue(Object o,String columnName);
    Object getOperObject();
    boolean init(Properties properties) throws SQLException, URISyntaxException;
    boolean close();
    boolean checkConnection() throws SQLException;
}
