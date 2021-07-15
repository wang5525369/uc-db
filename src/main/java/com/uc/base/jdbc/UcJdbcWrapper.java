package com.uc.base.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;

public interface UcJdbcWrapper extends Wrapper {
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(this.getClass());
    }

    default <T> T unwrap(Class<T> iface) throws SQLException {
        if (this.isWrapperFor(iface)) {
            return (T) this;
        } else {
            throw new SQLException();
        }
    }
}
