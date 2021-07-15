package com.uc.base.jdbc;


import com.uc.base.sql.UcType;

import java.util.Objects;

public class UcColumnInfo {
    public final String catalog;
    public final String schema;
    public final String table;
    public final String label;
    public final String name;
    public final int displaySize;
    public final UcType type;
    public final int index;

    public UcColumnInfo(String name, UcType type, String table, String catalog, String schema, String label, int displaySize, int index) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null");
        } else if (type == null) {
            throw new IllegalArgumentException("[type] must not be null");
        } else if (table == null) {
            throw new IllegalArgumentException("[table] must not be null");
        } else if (catalog == null) {
            throw new IllegalArgumentException("[catalog] must not be null");
        } else if (schema == null) {
            throw new IllegalArgumentException("[schema] must not be null");
        } else if (label == null) {
            throw new IllegalArgumentException("[label] must not be null");
        } else {
            this.name = name;
            this.type = type;
            this.table = table;
            this.catalog = catalog;
            this.schema = schema;
            this.label = label;
            this.displaySize = displaySize;
            this.index = index;
        }
    }

    public int displaySize() {
        return this.displaySize;
    }

    public String toString() {
        StringBuilder b = new StringBuilder();
        if (!"".equals(this.table)) {
            b.append(this.table).append('.');
        }

        b.append(this.name).append("<type=[").append(this.type).append(']');
        if (!"".equals(this.catalog)) {
            b.append(" catalog=[").append(this.catalog).append(']');
        }

        if (!"".equals(this.schema)) {
            b.append(" schema=[").append(this.schema).append(']');
        }

        if (!"".equals(this.label)) {
            b.append(" label=[").append(this.label).append(']');
        }

        return b.append('>').toString();
    }

    public boolean equals(Object obj) {
        if (obj != null && obj.getClass() == this.getClass()) {
            UcColumnInfo other = (UcColumnInfo)obj;
            return this.name.equals(other.name) && this.type.equals(other.type) && this.table.equals(other.table) && this.catalog.equals(other.catalog) && this.schema.equals(other.schema) && this.label.equals(other.label) && this.displaySize == other.displaySize;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.name, this.type, this.table, this.catalog, this.schema, this.label, this.displaySize});
    }
}
