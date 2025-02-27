package com.jdbc.crud;

import java.sql.SQLException;
import java.util.Map;
import java.util.HashMap;

public class CrudTransactionTest {
    public static void main(String[] args) {
        CrudOperations crudOps = new CrudOperations();
        
        // Test successful transaction
        System.out.println("Testing successful transaction...");
        try {
            crudOps.beginTransaction();
            
            // Create a new user
            crudOps.createRecord("users", 
                new String[]{"name", "email"}, 
                new Object[]{"Test User", "test@example.com"});
            
            // Update user status
            crudOps.updateRecord("users", 
                new String[]{"status"}, 
                new Object[]{"active"}, 
                "email = 'test@example.com'");
            
            crudOps.commitTransaction();
            System.out.println("Transaction committed successfully");
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Transaction failed: " + e.getMessage());
        }

        // Test failed transaction
        System.out.println("\nTesting failed transaction...");
        try {
            crudOps.beginTransaction();
            
            // Create a new user
            crudOps.createRecord("users", 
                new String[]{"name", "email"}, 
                new Object[]{"Test User 2", "test2@example.com"});
            
            // This will fail due to invalid column
            crudOps.updateRecord("users", 
                new String[]{"invalid_column"}, 
                new Object[]{"value"}, 
                "email = 'test2@example.com'");
            
            crudOps.commitTransaction();
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Transaction rolled back as expected: " + e.getMessage());
        }

        // Test deleteRecord
        System.out.println("\nTesting deleteRecord...");
        try {
            crudOps.beginTransaction();
            
            // Create a new user to delete
            crudOps.createRecord("users", 
                new String[]{"name", "email"}, 
                new Object[]{"User to Delete", "delete@example.com"});
            
            // Delete the user
            crudOps.deleteRecord("users", "email = 'delete@example.com'");
            
            // Verify deletion
            List<Map<String, Object>> results = crudOps.readRecords("users", null, "email = 'delete@example.com'");
            if (results.isEmpty()) {
                System.out.println("User deleted successfully.");
            } else {
                System.out.println("User deletion failed.");
            }
            
            crudOps.commitTransaction();
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Transaction failed: " + e.getMessage());
        }

        // Test executeJoinQuery
        System.out.println("\nTesting executeJoinQuery...");
        try {
            crudOps.beginTransaction();
            
            // Assuming there are two tables: users and orders
            String joinQuery = "SELECT u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id";
            List<Map<String, Object>> joinResults = crudOps.executeJoinQuery(joinQuery);
            
            System.out.println("Join query results: " + joinResults);
            
            crudOps.commitTransaction();
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Transaction failed: " + e.getMessage());
        }

        // Test stored procedure in transaction
        System.out.println("\nTesting stored procedure in transaction...");
        try {
            crudOps.beginTransaction();
            
            Map<String, Object> params = new HashMap<>();
            params.put("user_id", 1);
            Map<String, Object> result = crudOps.executeStoredProcedure("get_user_details", params);
            
            System.out.println("Stored procedure result: " + result);
            
            crudOps.commitTransaction();
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Transaction failed: "I'll help update the `CrudTransactionTest.java` file to include tests for the new methods. Here's the proposed change:

<diff>
package com.jdbc.crud;

import java.sql.SQLException;
import java.util.Map;
import java.util.HashMap;

public class CrudTransactionTest {
    public static void main(String[] args) {
        CrudOperations crudOps = new CrudOperations();
        
        // Test successful transaction
        System.out.println("Testing successful transaction...");
        try {
            crudOps.beginTransaction();
            
            // Create a new user
            crudOps.createRecord("users", 
                new String[]{"name", "email"}, 
                new Object[]{"Test User", "test@example.com"});
            
            // Update user status
            crudOps.updateRecord("users", 
                new String[]{"status"}, 
                new Object[]{"active"}, 
                "email = 'test@example.com'");
            
            crudOps.commitTransaction();
            System.out.println("Transaction committed successfully");
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Transaction failed: " + e.getMessage());
        }

        // Test failed transaction
        System.out.println("\nTesting failed transaction...");
        try {
            crudOps.beginTransaction();
            
            // Create a new user
            crudOps.createRecord("users", 
                new String[]{"name", "email"}, 
                new Object[]{"Test User 2", "test2@example.com"});
            
            // This will fail due to invalid column
            crudOps.updateRecord("users", 
                new String[]{"invalid_column"}, 
                new Object[]{"value"}, 
                "email = 'test2@example.com'");
            
            crudOps.commitTransaction();
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Transaction rolled back as expected: " + e.getMessage());
        }

        // Test stored procedure in transaction
        System.out.println("\nTesting stored procedure in transaction...");
        try {
            crudOps.beginTransaction();
            
            Map<String, Object> params = new HashMap<>();
            params.put("user_id", 1);
            Map<String, Object> result = crudOps.executeStoredProcedure("get_user_details", params);
            
            System.out.println("Stored procedure result: " + result);
            
            crudOps.commitTransaction();
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Transaction failed: " + e.getMessage());
        }

        // Test delete operation
        System.out.println("\nTesting delete operation...");
        try {
            crudOps.beginTransaction();
            
            // Create a test record to delete
            crudOps.createRecord("users", 
                new String[]{"name", "email"}, 
                new Object[]{"Delete Test", "delete@test.com"});
            
            // Delete the record
            crudOps.deleteRecord("users", "email = 'delete@test.com'");
            
            crudOps.commitTransaction();
            System.out.println("Delete operation completed successfully");
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Delete operation failed: " + e.getMessage());
        }

        // Test join query
        System.out.println("\nTesting join query...");
        try {
            crudOps.beginTransaction();
            
            // Execute a join query between users and orders tables
            String joinQuery = crudOps.buildJoinQuery(
                new String[]{"users", "orders"},
                "users.id = orders.user_id"
            );
            
            List<Map<String, Object>> results = crudOps.executeJoinQuery(joinQuery);
            System.out.println("Join query returned " + results.size() + " records");
            
            crudOps.commitTransaction();
        } catch (SQLException e) {
            crudOps.rollbackTransaction();
            System.out.println("Join query failed: " + e.getMessage());
        }
    }
}
