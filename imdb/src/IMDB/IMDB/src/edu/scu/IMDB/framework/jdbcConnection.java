/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.scu.IMDB.framework;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author visa
 */
public class jdbcConnection {

    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {

            //System.out.println("Where is your Oracle JDBC Driver?");
            e.printStackTrace();

        }

        //System.out.println("Oracle JDBC Driver Registered!");
        Connection connection = null;

        try {

            connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@192.168.56.101:1521:imdb", "visa",
                    "visa1234");

        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();

        }

        if (connection != null) {
            //System.out.println("You made it, take control your database now!");

        } else {
            System.out.println("Failed to make connection!");
        }
        return connection;
    }

}
