package com.jdbc.crud;

import com.jdbc.crud.CrudException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.jdbc.crud.LoggerUtil;

public class CrudOperations {
    private static final Logger logger = Logger.getLogger(CrudOperations.class.getName());
    private Connection transactionConnection = null;
    private boolean inTransaction = false;

    static {
        LoggerUtil.configureLogger(logger);
    }

    public void beginTransaction() throws SQLException {
        if (inTransaction) {
            throw new SQLException("Transaction already in progress");
        }
        transactionConnection = DatabaseConnectionManager.getConnection();
        transactionConnection.setAutoCommit(false);
        inTransaction = true;
        logger.log(Level.INFO, "Transaction started");
    }

    public void commitTransaction() throws SQLException {
        if (!inTransaction || transactionConnection == null) {
            throw new SQLException("No active transaction to commit");
        }
        try {
            transactionConnection.commit();
            logger.log(Level.INFO, "Transaction committed successfully");
        } finally {
            cleanupTransaction();
        }
    }

    public void rollbackTransaction() {
        if (inTransaction && transactionConnection != null) {
            try {
                transactionConnection.rollback();
                logger.log(Level.INFO, "Transaction rolled back");
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Error during transaction rollback", e);
            } finally {
                cleanupTransaction();
            }
        }
    }

    private void cleanupTransaction() {
        if (transactionConnection != null) {
            try {
                transactionConnection.setAutoCommit(true);
                DatabaseConnectionManager.closeConnection(transactionConnection);
            } catch (SQLException e) {
                logger.log(Level.WARNING, "Error cleaning up transaction", e);
            }
            transactionConnection = null;
            inTransaction = false;
        }
    }

    public void createRecord(String tableName, String[] columns, Object[] values) {
        if (columns == null || values == null || columns.length != values.length) {
            logger.log(Level.SEVERE, "Columns and values must be non-null and of equal length");
            throw new IllegalArgumentException("Columns and values must be non-null and of equal length");
        }

        StringBuilder sql = new StringBuilder("INSERT INTO ")
            .append(tableName)
            .append(" (")
            .append(String.join(", ", columns))
            .append(") VALUES (")
            .append("?, ".repeat(columns.length))
            .delete(sql.length() - 2, sql.length()) // Remove last comma and space
            .append(")");

        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < values.length; i++) {
                pstmt.setObject(i + 1, values[i]);
            }
            
            int rowsAffected = pstmt.executeUpdate();
            logger.log(Level.INFO, "Successfully created record in table {0}. Rows affected: {1}", 
                new Object[]{tableName, rowsAffected});
        } catch (SQLException e) {
            handleSQLException(e);
            throw new CrudException("Failed to create record: " + e.getMessage(), e);
        } finally {
            if (!inTransaction) {
                DatabaseConnectionManager.closeConnection(conn);
            }
        }
    }

    public List<Map<String, Object>> readRecords(String tableName, String[] columns, String whereClause) {
        String sql = buildSelectQuery(tableName, columns, whereClause);
        List<Map<String, Object>> results = new ArrayList<>();
        
        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
            
            logger.log(Level.INFO, "Successfully read {0} records from table {1}", 
                new Object[]{results.size(), tableName});
            return results;
        } catch (SQLException e) {
            handleSQLException(e);
            throw new CrudException("Failed to read records: " + e.getMessage(), e);
        } finally {
            if (!inTransaction) {
                DatabaseConnectionManager.closeConnection(conn);
            }
        }
    }

    private Connection getConnection() {
        return inTransaction ? transactionConnection : DatabaseConnectionManager.getConnection();
    }

    private void handleSQLException(SQLException e) throws CrudException {
        if (inTransaction) {
            rollbackTransaction();
        }
        logger.log(Level.SEVERE, "SQL error occurred", e);
        throw new CrudException("Database operation failed: " + e.getMessage(), e);
    }

    public void updateRecord(String tableName, String[] columns, Object[] values, String whereClause) {
        if (columns == null || values == null || columns.length != values.length) {
            logger.log(Level.SEVERE, "Columns and values must be non-null and of equal length");
            throw new IllegalArgumentException("Columns and values must be non-null and of equal length");
        }

        StringBuilder sql = new StringBuilder("UPDATE ")
            .append(tableName)
            .append(" SET ")
            .append(String.join(" = ?, ", columns))
            .append(" = ?")
            .append(" WHERE ")
            .append(whereClause);

        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < values.length; i++) {
                pstmt.setObject(i + 1, values[i]);
            }
            
            int rowsAffected = pstmt.executeUpdate();
            logger.log(Level.INFO, "Successfully updated record in table {0}. Rows affected: {1}", 
                new Object[]{tableName, rowsAffected});
        } catch (SQLException e) {
            handleSQLException(e);
            throw new CrudException("Failed to update record: " + e.getMessage(), e);
        } finally {
            if (!inTransaction) {
                DatabaseConnectionManager.closeConnection(conn);
            }
        }
    }

    public void deleteRecord(String tableName, String whereClause) {
        String sql = "DELETE FROM " + tableName + " WHERE " + whereClause;

        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int rowsAffected = pstmt.executeUpdate();
            logger.log(Level.INFO, "Successfully deleted record from table {0}. Rows affected: {1}", 
                new Object[]{tableName, rowsAffected});
        } catch (SQLException e) {
            handleSQLException(e);
            throw new CrudException("Failed to delete record: " + e.getMessage(), e);
        } finally {
            if (!inTransaction) {
                DatabaseConnectionManager.closeConnection(conn);
            }
        }
    }

    public List<Map<String, Object>> executeJoinQuery(String joinQuery) {
        List<Map<String, Object>> results = new ArrayList<>();
        
        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(joinQuery);
             ResultSet rs = pstmt.executeQuery()) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
            
            logger.log(Level.INFO, "Successfully executed join query. Records returned: {0}", 
                results.size());
            return results;
        } catch (SQLException e) {
            handleSQLException(e);
            throw new CrudException("Failed to execute join query: " + e.getMessage(), e);
        } finally {
            if (!inTransaction) {
                DatabaseConnectionManager.closeConnection(conn);
            }
        }
    }

    public void executeStoredProcedure(String procedureName, Object... params) {
        StringBuilder sql = new StringBuilder("{call ").append(procedureName).append("(");
        sql.append("?,".repeat(params.length)).deleteCharAt(sql.length() - 1).append(")}");

        Connection conn = getConnection();
        try (CallableStatement cstmt = conn.prepareCall(sql.toString())) {
            for (int i = 0; i < params.length; i++) {
                cstmt.setObject(i + 1, params[i]);
            }
            cstmt.execute();
            logger.log(Level.INFO, "Successfully executed stored procedure {0}", procedureName);
        } catch (SQLException e) {
            handleSQLException(e);
            throw new CrudException("Failed to execute stored procedure: " + e.getMessage(), e);
        } finally {
            if (!inTransaction) {
                DatabaseConnectionManager.closeConnection(conn);
            }
        }
    }

    private String buildSelectQuery(String tableName, String[] columns, String whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ")
            .append(columns != null ? String.join(", ", columns) : "*")
            .append(" FROM ")
            .append(tableName);
        
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        return sql.toString();
    }

    private String buildUpdateQuery(String tableName, String[] columns, String whereClause) {
        return new StringBuilder("UPDATE ")
            .append(tableName)
            .append(" SET ")
            .append(String.join(" = ?, ", columns))
            .append(" = ?")
            .append(" WHERE ")
            .append(whereClause)
            .toString();
    }

    private String buildJoinQuery(String[] tables, String joinCondition) {
        return new StringBuilder("SELECT * FROM ")
            .append(String.join(" JOIN ", tables))
            .append(" ON ")
            .append(joinCondition)
            .toString();
    }
}
