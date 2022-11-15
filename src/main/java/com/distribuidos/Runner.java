package com.distribuidos;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Runner {
    public static void main(String[] args) throws SQLException {
        var logger = Logger.getLogger("Runner");
        logger.log(Level.INFO, "Starting server...");
        Server server = new Server();
        logger.log(Level.INFO,"Connected to database...");
        int tries = 0;
        while(!Thread.currentThread().isInterrupted()){
            try{
                server.run();
                tries = 0;
            }catch (Exception e){
                logger.log(Level.SEVERE,"Severe exception");
                e.printStackTrace();
                System.out.println();
                tries++;
                if(tries == 4){
                    break;
                }
            }
        }
    }
}