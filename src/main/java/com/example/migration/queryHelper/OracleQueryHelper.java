package com.example.migration.queryHelper;


public class OracleQueryHelper {

    public final static String SELECT_ALL_TABLES = "SELECT table_name FROM all_tables WHERE owner = ?";
    public final static String SELECT_TABLE_INFO = "SELECT tabcols.column_id, tabcols.column_name, tabcols.data_type, " +
            "tabcols.data_length, cons.constraint_type, cons.constraint_name, cons.search_condition, tabcols.NULLABLE\n" +
            "FROM all_tab_cols tabcols\n" +
            "LEFT JOIN all_cons_columns cols\n" +
            "  ON tabcols.owner = cols.owner\n" +
            " AND tabcols.table_name = cols.table_name\n" +
            " AND tabcols.column_name = cols.column_name\n" +
            "LEFT JOIN all_constraints cons\n" +
            "  ON cols.owner = cons.owner\n" +
            " AND cols.constraint_name = cons.constraint_name\n" +
            "WHERE tabcols.owner = ?\n" +
            " AND tabcols.table_name = ?\n" +
            "ORDER BY tabcols.column_id";
}
