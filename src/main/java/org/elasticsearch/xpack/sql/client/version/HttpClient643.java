package org.elasticsearch.xpack.sql.client.version;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.xpack.sql.client.*;
import org.elasticsearch.xpack.sql.proto.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.Collections;
import java.util.TimeZone;
import java.util.function.Function;

public class HttpClient643
{
//    private static final XContentType REQUEST_BODY_CONTENT_TYPE;
//    private final ConnectionConfiguration cfg;
//    private NamedXContentRegistry registry;
//
//    public HttpClient643(final ConnectionConfiguration cfg) {
//        this.registry = NamedXContentRegistry.EMPTY;
//        this.cfg = cfg;
//    }
//
//    public boolean ping(final long timeoutInMs) throws SQLException {
//        return this.head("/", timeoutInMs);
//    }
//
//    public MainResponse serverInfo() throws SQLException {
//        return this.get("/", MainResponse::fromXContent);
//    }
//
//    public SqlQueryResponse queryInit(final String query, final int fetchSize) throws SQLException {
//        final SqlQueryRequest sqlRequest = new SqlQueryRequest(Mode.PLAIN, query, Collections.emptyList(), null, TimeZone.getTimeZone("UTC"), fetchSize, TimeValue.timeValueMillis(this.cfg.queryTimeout()), TimeValue.timeValueMillis(this.cfg.pageTimeout()));
//        return this.query(sqlRequest);
//    }
//
//    public SqlQueryResponse query(final SqlQueryRequest sqlRequest) throws SQLException {
//        return this.post("/_xpack/sql", sqlRequest, SqlQueryResponse::fromXContent);
//    }
//
//    public SqlQueryResponse nextPage(final String cursor) throws SQLException {
//        final SqlQueryRequest sqlRequest = new SqlQueryRequest(Mode.PLAIN, cursor, TimeValue.timeValueMillis(this.cfg.queryTimeout()), TimeValue.timeValueMillis(this.cfg.pageTimeout()));
//        return this.post("/_xpack/sql", sqlRequest, SqlQueryResponse::fromXContent);
//    }
//
//    public boolean queryClose(final String cursor) throws SQLException {
//        final SqlClearCursorResponse response = this.post("/_xpack/sql/close", new SqlClearCursorRequest(Mode.PLAIN, cursor), SqlClearCursorResponse::fromXContent);
//        return response.isSucceeded();
//    }
//
//    private <Request extends AbstractSqlRequest, Response> Response post(final String path, final Request request, final CheckedFunction<XContentParser, Response, IOException> responseParser) throws SQLException {
//        final byte[] requestBytes = toXContent(request);
//        final String query = "error_trace&mode=" + "plain";
//        //final String query = "error_trace&mode=" + request.mode();
//        //final Tuple<XContentType, byte[]> response = AccessController.doPrivileged(() -> JreHttpUrlConnection.http(path, query, this.cfg, con -> con.request(out -> out.write(requestBytes), this::readFrom, "POST"))).getResponseOrThrowException();
//        CheckedBiFunction<InputStream, Function<String, String>, Tuple<XContentType, byte[]>, IOException> f1 = this::readFrom;
//        JreHttpUrlConnection.ResponseOrException<Tuple<XContentType, byte[]>> responseOrException = JreHttpUrlConnection.http(path, query, this.cfg, con -> con.request(out -> out.write(requestBytes), f1, "POST"));
//        PrivilegedAction<JreHttpUrlConnection.ResponseOrException<Tuple<XContentType, byte[]>>> privilegedAction = new PrivilegedAction<JreHttpUrlConnection.ResponseOrException<Tuple<XContentType, byte[]>>>() {
//            public JreHttpUrlConnection.ResponseOrException<Tuple<XContentType, byte[]>> run() {
//                return responseOrException;
//            }
//        };
//        final Tuple<XContentType, byte[]> response = AccessController.doPrivileged(privilegedAction).getResponseOrThrowException();
//
//        return this.fromXContent(response.v1(), response.v2(), responseParser);
//    }
//
//    private boolean head(final String path, final long timeoutInMs) throws SQLException {
//        final ConnectionConfiguration pingCfg = new ConnectionConfiguration(this.cfg.baseUri(), this.cfg.connectionString(), this.cfg.connectTimeout(), timeoutInMs, this.cfg.queryTimeout(), this.cfg.pageTimeout(), this.cfg.pageSize(), this.cfg.authUser(), this.cfg.authPass(), this.cfg.sslConfig(), this.cfg.proxyConfig());
//        try {
//            Function<JreHttpUrlConnection,Boolean> f = JreHttpUrlConnection::head;
//            boolean bRet = JreHttpUrlConnection.http(path, "error_trace", pingCfg,f);
//            PrivilegedAction<Boolean> privilegedAction = new PrivilegedAction<Boolean>() {
//                public Boolean run() {
//                    return bRet;
//                }
//            };
//            return AccessController.doPrivileged(privilegedAction);
//            //return AccessController.doPrivileged(() -> JreHttpUrlConnection.http(path, "error_trace", pingCfg, JreHttpUrlConnection::head));
//        }
//        catch (ClientException ex) {
//            throw new SQLException("Cannot ping server", ex);
//        }
//    }
//
//    private <Response> Response get(final String path, final CheckedFunction<XContentParser, Response, IOException> responseParser) throws SQLException {
//        //final Tuple<XContentType, byte[]> response = AccessController.doPrivileged(() -> JreHttpUrlConnection.http(path, "error_trace", this.cfg, con -> con.request(null, this::readFrom, "GET"))).getResponseOrThrowException();
//        CheckedBiFunction<InputStream, Function<String, String>, Tuple<XContentType, byte[]>, IOException> f1 = this::readFrom;
//        JreHttpUrlConnection.ResponseOrException<Tuple<XContentType, byte[]>> responseOrException = JreHttpUrlConnection.http(path, "error_trace", this.cfg,con -> con.request(null, f1, "GET"));
//        PrivilegedAction<JreHttpUrlConnection.ResponseOrException<Tuple<XContentType, byte[]>>> privilegedAction = new PrivilegedAction<JreHttpUrlConnection.ResponseOrException<Tuple<XContentType, byte[]>>>() {
//            public JreHttpUrlConnection.ResponseOrException<Tuple<XContentType, byte[]>> run() {
//                return responseOrException;
//            }
//        };
//        final Tuple<XContentType, byte[]> response = AccessController.doPrivileged(privilegedAction).getResponseOrThrowException();
//        return this.fromXContent(response.v1(), response.v2(), responseParser);
//    }
//
//    private static <Request extends ToXContent> byte[] toXContent(final Request xContent) {
//        try {
//            final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//            Throwable x0 = null;
//            try {
//                final XContentBuilder xContentBuilder = new XContentBuilder(HttpClient643.REQUEST_BODY_CONTENT_TYPE.xContent(), buffer);
//                Throwable x2 = null;
//                try {
//                    if (xContent.isFragment()) {
//                        xContentBuilder.startObject();
//                    }
//                    xContent.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
//                    if (xContent.isFragment()) {
//                        xContentBuilder.endObject();
//                    }
//                }
//                catch (Throwable t) {
//                    x2 = t;
//                    throw t;
//                }
//                finally {
//                    $closeResource(x2, xContentBuilder);
//                }
//                return buffer.toByteArray();
//            }
//            catch (Throwable t2) {
//                x0 = t2;
//                throw t2;
//            }
//            finally {
//                $closeResource(x0, buffer);
//            }
//        }
//        catch (IOException ex) {
//            throw new ClientException("Cannot serialize request", ex);
//        }
//    }
//
//    private Tuple<XContentType, byte[]> readFrom(final InputStream inputStream, final Function<String, String> headers) {
//        final String contentType = headers.apply("Content-Type");
//        final XContentType xContentType = XContentType.fromMediaTypeOrFormat(contentType);
//        if (xContentType == null) {
//            throw new IllegalStateException("Unsupported Content-Type: " + contentType);
//        }
//        final ByteArrayOutputStream out = new ByteArrayOutputStream();
//        try {
//            Streams.copy(inputStream, out);
//        }
//        catch (IOException ex) {
//            throw new ClientException("Cannot deserialize response", ex);
//        }
//        return new Tuple<XContentType, byte[]>(xContentType, out.toByteArray());
//    }
//
//    private <Response> Response fromXContent(final XContentType xContentType, final byte[] bytesReference, final CheckedFunction<XContentParser, Response, IOException> responseParser) {
//        try {
//            final InputStream stream = new ByteArrayInputStream(bytesReference);
//            Throwable x0 = null;
//            try {
//                final XContentParser parser = xContentType.xContent().createParser(this.registry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream);
//                Throwable x2 = null;
//                try {
//                    return responseParser.apply(parser);
//                }
//                catch (Throwable t) {
//                    x2 = t;
//                    throw t;
//                }
//                finally {
//                    if (parser != null) {
//                        $closeResource(x2, parser);
//                    }
//                }
//            }
//            catch (Throwable t2) {
//                x0 = t2;
//                throw t2;
//            }
//            finally {
//                $closeResource(x0, stream);
//            }
//        }
//        catch (IOException ex) {
//            throw new ClientException("Cannot parse response", ex);
//        }
//    }
//
//    private static /* synthetic */ void $closeResource(final Throwable x0, final AutoCloseable x1) {
//        if (x0 != null) {
//            try {
//                x1.close();
//            }
//            catch (Throwable t) {
//                x0.addSuppressed(t);
//            }
//        }
//        else {
//            try {
//                x1.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    static {
//        REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;
//    }
}
