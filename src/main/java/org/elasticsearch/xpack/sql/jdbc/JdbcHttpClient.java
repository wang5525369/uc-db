package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.client.Version;
import org.elasticsearch.xpack.sql.proto.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class JdbcHttpClient
{
    private final HttpClient httpClient;
    private final JdbcConfiguration conCfg;
    private final InfoResponse serverInfo;

    JdbcHttpClient(final JdbcConfiguration conCfg) throws SQLException {
        this.httpClient = new HttpClient(conCfg);
        this.conCfg = conCfg;
        this.serverInfo = this.fetchServerInfo();
        this.checkServerVersion();
    }

    boolean ping(final long timeoutInMs) throws SQLException {
        return this.httpClient.ping(timeoutInMs);
    }

    Cursor query(final String sql, final List<SqlTypedParamValue> params, final RequestMeta meta) throws SQLException {
        final int fetch = (meta.fetchSize() > 0) ? meta.fetchSize() : this.conCfg.pageSize();
        final SqlQueryRequest sqlRequest = new SqlQueryRequest(sql,TimeValue.timeValueMillis(meta.timeoutInMs()), TimeValue.timeValueMillis(meta.queryTimeoutInMs()), new RequestInfo(Mode.JDBC));
        final SqlQueryResponse response = this.httpClient.query(sqlRequest);
        return new DefaultCursor(this, response.cursor(), this.toJdbcColumnInfo(response.columns()), response.rows(), meta);
    }

    Tuple<String, List<List<Object>>> nextPage(final String cursor, final RequestMeta meta) throws SQLException {
        final SqlQueryRequest sqlRequest = new SqlQueryRequest(cursor, TimeValue.timeValueMillis(meta.timeoutInMs()), TimeValue.timeValueMillis(meta.queryTimeoutInMs()), new RequestInfo(Mode.JDBC));
        final SqlQueryResponse response = this.httpClient.query(sqlRequest);
        return new Tuple<String, List<List<Object>>>(response.cursor(), response.rows());
    }

    boolean queryClose(final String cursor) throws SQLException {
        return this.httpClient.queryClose(cursor);
    }

    InfoResponse serverInfo() throws SQLException {
        return this.serverInfo;
    }

    private InfoResponse fetchServerInfo() throws SQLException {
        final MainResponse mainResponse = this.httpClient.serverInfo();
        final Version version = Version.fromString(mainResponse.getVersion());
        return new InfoResponse(mainResponse.getClusterName(), version.major, version.minor, version.revision);
    }

    private void checkServerVersion() throws SQLException {
        if (this.serverInfo.majorVersion != Version.CURRENT.major || this.serverInfo.minorVersion != Version.CURRENT.minor || this.serverInfo.revisionVersion != Version.CURRENT.revision) {
            //throw new SQLException("This version of the JDBC driver is only compatible with Elasticsearch version " + Version.CURRENT.toString() + ", attempting to connect to a server version " + this.serverInfo.versionString());
        }
    }

    private List<JdbcColumnInfo> toJdbcColumnInfo(final List<ColumnInfo> columns) throws SQLException {
        final List<JdbcColumnInfo> cols = new ArrayList<JdbcColumnInfo>(columns.size());
        for (final ColumnInfo info : columns) {
            cols.add(new JdbcColumnInfo(info.name(), TypeUtils.of(info.esType()), "", "", "", "", info.displaySize()));
        }
        return cols;
    }
}
