package com.uc.base.enums;

public class UcSqlEnums {
    public enum SQL_TOKEN {
        NULL(0,"null"),
        OR(1, "or"),
        AND(2, "and"),
        LR(3, "("),
        RR(4, ")"),
        EQ(5, "="),
        NE(6,"<>"),
        GT(7, ">"),
        GTE(8, ">="),
        LT(9, "<"),
        LTE(10, "<="),
        BETWEEN(11, "between"),
        NOT_BETWEEN(12, "notbetween"),
        IN(13, "in"),
        NOT_IN(14, "notin"),
        LIKE(15, "like"),
        NOT_LIKE(16, "notlike");

        private int code;
        private String name;

        SQL_TOKEN(int code, String name) {
            this.code = code;
            this.name = name;
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public static SQL_TOKEN getByName(String name) {
            String temp = name.toLowerCase();
            for (SQL_TOKEN sql_token : SQL_TOKEN.values()) {
                if (sql_token.getName().equals(temp) == true) {
                    return sql_token;
                }
            }
            return null;
        }
    }
}
