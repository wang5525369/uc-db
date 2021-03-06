package com.uc.base.jdbc;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uc.base.oper.DbOper;
import com.uc.base.sql.UcType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class UcDatabaseMetaData implements DatabaseMetaData,UcJdbcWrapper {
    private UcConnection ucConnection = null;

    public UcDatabaseMetaData(Connection connection){
        this.ucConnection = (UcConnection) connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return this.ucConnection.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        return this.ucConnection.getUserName();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return true;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return true;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return null;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return null;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        UcResultSet ucResultSet = new UcResultSet(ucConnection);
        if (StringUtils.isEmpty(tableNamePattern) == true){
            throw new SQLException("???????????????");
        }
        List<UcTableInfo> listUcTableInfo = getTables(catalog,schemaPattern,tableNamePattern);

        if ("%".equals(tableNamePattern) == true){
            DbOper dbOper = ucConnection.getDbOper();
            listUcTableInfo = dbOper.getTables();

        }else{
            String [] arrayTables = tableNamePattern.split(";");
            for(String tableName : arrayTables){
                UcTableInfo ucTableInfo = new UcTableInfo();
                ucTableInfo.setTableName(tableName);
                listUcTableInfo.add(ucTableInfo);
            }
        }
        List<UcColumnInfo> listUcColumnInfo = initTableInfo();
        List<Object> listObject = Lists.newArrayList();
        for(UcTableInfo ucTableInfo : listUcTableInfo){
            Map<String,Object> mapObject = Maps.newHashMap();
            mapObject.put("TABLE_NAME",ucTableInfo.getTableName());
            mapObject.put("TABLE_TYPE","");
            mapObject.put("REMARKS","");
            String jsonString = JSONObject.toJSONString(mapObject);
            listObject.add(jsonString);
        }

        ucResultSet.convertResultSet(listUcColumnInfo, listObject);
        return ucResultSet;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        DbOper dbOper = ucConnection.getDbOper();
        List<UcTableInfo> listUcTableInfo = getTables(catalog,schemaPattern,tableNamePattern);
        List<UcColumnInfo> listTableColumnInfo = initColumInfo();
        List<Object> listObject = Lists.newArrayList();
        for(UcTableInfo ucTableInfo : listUcTableInfo){
            String tableName = ucTableInfo.getTableName();
            Pair<String,List<UcColumnInfo>> pair = dbOper.getColumnInfo(tableName);
            List<UcColumnInfo> listUcColumnInfo = pair.getRight();
            for(UcColumnInfo ucColumnInfo : listUcColumnInfo){
                Map<String,Object> mapObject = Maps.newHashMap();
                mapObject.put("TABLE_CAT",catalog);
                mapObject.put("TABLE_SCHEM",schemaPattern);
                mapObject.put("TABLE_NAME",tableName);
                mapObject.put("DATA_TYPE",ucColumnInfo.type.getType());
                mapObject.put("TYPE_NAME",ucColumnInfo.type.getName());
                mapObject.put("COLUMN_SIZE",100);
                mapObject.put("COLUMN_NAME",ucColumnInfo.name);
                mapObject.put("NULLABLE",DatabaseMetaData.columnNullable);
                mapObject.put("DECIMAL_DIGITS",2);
                mapObject.put("REMARKS","");
                mapObject.put("COLUMN_DEF","");
                mapObject.put("IS_AUTOINCREMENT", "NO");
                mapObject.put("IS_GENERATEDCOLUMN", "NO");
                mapObject.put("KEY_SEQ", 1);
                mapObject.put("PK_NAME", "");
                String jsonString = JSONObject.toJSONString(mapObject);
                listObject.add(jsonString);
            }
        }

        UcResultSet ucResultSet = new UcResultSet(ucConnection);
        ucResultSet.convertResultSet(listTableColumnInfo,listObject);
        return ucResultSet;
    }

    private  List<UcColumnInfo> initColumInfo(){
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        String [] arrayColumInfo = {"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","DATA_TYPE","TYPE_NAME","COLUMN_SIZE","COLUMN_NAME","NULLABLE","DECIMAL_DIGITS","REMARKS","COLUMN_DEF","IS_AUTOINCREMENT","IS_GENERATEDCOLUMN","KEY_SEQ","PK_NAME"};

        int index = 1;
        for(String name : arrayColumInfo){
            String type = "STRING";
            if (name.equals("DECIMAL_DIGITS") == true || name.equals("DATA_TYPE") == true || name.equals("COLUMN_SIZE") == true || name.equals("NULLABLE") == true){
                type = "INTEGER";
            }else if (name.equals("KEY_SEQ") == true) {
                type = "SHORT";
            }
            UcColumnInfo ucColumnInfo = new UcColumnInfo(name, UcType.valueOf(type),name,name,name,name,1000,index);
            listUcColumnInfo.add(ucColumnInfo);
            index++;
        }
        return listUcColumnInfo;
    }

    private  List<UcColumnInfo> initTableInfo(){
        List<UcColumnInfo> listUcColumnInfo = Lists.newArrayList();
        String [] arrayColumInfo = {"TABLE_NAME","TABLE_TYPE","REMARKS"};

        int index = 1;
        for(String name : arrayColumInfo){
            String type = "STRING";
            UcColumnInfo ucColumnInfo = new UcColumnInfo(name, UcType.valueOf(type),name,name,name,name,1000,index);
            listUcColumnInfo.add(ucColumnInfo);
            index++;
        }
        return listUcColumnInfo;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        DbOper dbOper = ucConnection.getDbOper();
        List<UcTableInfo> listUcTableInfo = getTables(catalog,schema,table);
        List<UcColumnInfo> listTableColumnInfo = initColumInfo();
        List<Object> listObject = Lists.newArrayList();
        for(UcTableInfo ucTableInfo : listUcTableInfo){
            String tableName = ucTableInfo.getTableName();
            Pair<String,List<UcColumnInfo>> pair = dbOper.getColumnInfo(tableName);
            List<UcColumnInfo> listUcColumnInfo = pair.getRight();
            Map<String,Object> mapObject = Maps.newHashMap();
            for(UcColumnInfo ucColumnInfo : listUcColumnInfo){
                if ((ucColumnInfo.type == UcType.KEYWORD && ucColumnInfo.name.equals("id") == true ) || (ucColumnInfo.type == UcType.OBJECTID && ucColumnInfo.name.equals("_id") == true)) {
                    mapObject.put("TABLE_CAT",catalog);
                    mapObject.put("TABLE_SCHEM",schema);
                    mapObject.put("TABLE_NAME",tableName);
                    mapObject.put("DATA_TYPE",ucColumnInfo.type.getType());
                    mapObject.put("TYPE_NAME",ucColumnInfo.type.getName());
                    mapObject.put("COLUMN_SIZE",100);
                    mapObject.put("COLUMN_NAME",ucColumnInfo.name);
                    mapObject.put("NULLABLE",DatabaseMetaData.columnNullable);
                    //mapObject.put("DECIMAL_DIGITS",2);
                    mapObject.put("REMARKS","");
                    mapObject.put("COLUMN_DEF","");
                    mapObject.put("IS_AUTOINCREMENT", "NO");
                    mapObject.put("IS_GENERATEDCOLUMN", "NO");
                    mapObject.put("KEY_SEQ", 1);
                    mapObject.put("PK_NAME", "");
                    break;
                }
            }
            String jsonString = JSONObject.toJSONString(mapObject);
            listObject.add(jsonString);
        }

        UcResultSet ucResultSet = new UcResultSet(ucConnection);
        ucResultSet.convertResultSet(listTableColumnInfo,listObject);
        return ucResultSet;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    private List<UcTableInfo> getTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        List<UcTableInfo> listUcTableInfo = Lists.newArrayList();
        if (StringUtils.isEmpty(tableNamePattern) == true){
            throw new SQLException("???????????????");
        }

        if ("%".equals(tableNamePattern) == true){
            DbOper dbOper = ucConnection.getDbOper();
            listUcTableInfo = dbOper.getTables();

        }else{
            String [] arrayTables = tableNamePattern.split(";");
            for(String tableName : arrayTables){
                UcTableInfo ucTableInfo = new UcTableInfo();
                ucTableInfo.setTableName(tableName);
                listUcTableInfo.add(ucTableInfo);
            }
        }
        return listUcTableInfo;
    }
}
