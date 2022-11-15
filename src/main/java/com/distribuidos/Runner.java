package com.distribuidos;

import java.sql.SQLException;

public class Runner {
    public static void main(String[] args) throws SQLException {
        Server server = new Server();
        int tries = 0;
        while(!Thread.currentThread().isInterrupted()){
            try{
                server.run();
                tries = 0;
            }catch (Exception e){
                e.printStackTrace();
                tries++;
                if(tries == 4){
                    break;
                }
            }
        }
    }
}