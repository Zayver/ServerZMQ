package com.distribuidos;

import java.sql.*;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.zeromq.*;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

//
// Paranoid Pirate worker
//
public class Server {
    private final static int HEARTBEAT_LIVENESS = 3;     //  3-5 is reasonable
    private final static int HEARTBEAT_INTERVAL = 1000;  //  msecs
    private final static int INTERVAL_INIT = 1000;  //  Initial reconnect
    private final static int INTERVAL_MAX = 32000; //  After exponential backoff

    //  Paranoid Pirate Protocol constants
    private final static String PPP_READY = "\u0001"; //  Signals worker is ready
    private final static String PPP_HEARTBEAT = "\u0002"; //  Signals worker heartbeat

    private final Connection con;

    Server() throws SQLException {
        con = DriverManager.getConnection("jdbc:mysql://db-mysql-nyc1-26039-do-user-12425800-0.b.db.ondigitalocean.com:25060/defaultdb", "doadmin", "AVNS_xSBmY-Vlbi_ADXbmXmT");
    }


    private static Socket worker_socket(ZContext ctx) {
        Socket worker = ctx.createSocket(SocketType.DEALER);
        worker.connect("tcp://localhost:5556");

        //  Tell queue we're ready for work
        System.out.println("I: worker ready\n");
        ZFrame frame = new ZFrame(PPP_READY);
        frame.send(worker, 0);

        return worker;
    }

    public void run() {
        try (ZContext ctx = new ZContext()) {
            Socket worker = worker_socket(ctx);

            Poller poller = ctx.createPoller(1);
            poller.register(worker, Poller.POLLIN);

            int liveness = HEARTBEAT_LIVENESS;
            int interval = INTERVAL_INIT;

            long heartbeat_at = System.currentTimeMillis() + HEARTBEAT_INTERVAL;

            while (true) {
                int rc = poller.poll(HEARTBEAT_INTERVAL);
                if (rc == -1)
                    break; //  Interrupted

                if (poller.pollin(0)) {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    if (msg == null)
                        break; //  Interrupted


                    if (msg.size() == 3) {
                        System.out.println("I: normal reply\n");
                        var result = operation(msg.getLast().toString());
                        msg.add(result);
                        msg.send(worker);
                        liveness = HEARTBEAT_LIVENESS;
                    } else if (msg.size() == 1) {
                        ZFrame frame = msg.getFirst();
                        String frameData = new String(
                                frame.getData(), ZMQ.CHARSET
                        );
                        if (PPP_HEARTBEAT.equals(frameData))
                            liveness = HEARTBEAT_LIVENESS;
                        else {
                            System.out.println("E: invalid message\n");
                            msg.dump(System.out);
                        }
                        msg.destroy();
                    } else {
                        System.out.println("E: invalid message\n");
                        msg.dump(System.out);
                    }
                    interval = INTERVAL_INIT;
                } else if (--liveness == 0) {
                    System.out.println(
                            "W: heartbeat failure, can't reach queue\n"
                    );
                    System.out.printf(
                            "W: reconnecting in %sd msec\n", interval
                    );
                    try {
                        Thread.sleep(interval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (interval < INTERVAL_MAX)
                        interval *= 2;
                    ctx.destroySocket(worker);
                    worker = worker_socket(ctx);
                    liveness = HEARTBEAT_LIVENESS;
                }

                //  Send heartbeat to queue if it's time
                if (System.currentTimeMillis() > heartbeat_at) {
                    long now = System.currentTimeMillis();
                    heartbeat_at = now + HEARTBEAT_INTERVAL;
                    System.out.println("I: worker heartbeat\n");
                    ZFrame frame = new ZFrame(PPP_HEARTBEAT);
                    frame.send(worker, 0);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private String operation(String data) throws SQLException {
        JsonObject query =  new Gson().fromJson(data, JsonObject.class);
        String operacion = query.get("operacion").getAsString();

        if(Objects.equals(operacion, "consulta")){
            return getAllProducts();
        }
        return "";
    }


    private String getAllProducts() throws SQLException {
        String sql = "SELECT NAME, PRICE, AMOUNT, DESCRIPTION FROM PRODUCT";
        Statement stat = con.createStatement();
        var rs = stat.executeQuery(sql);
        JsonArray arr = new JsonArray();
        while (rs.next()) {
            var obj = new JsonObject();
            obj.addProperty("nombre", rs.getString(1));
            obj.addProperty("precio", rs.getDouble(2));
            obj.addProperty("amount", rs.getInt(3));
            obj.addProperty("descripcion", rs.getInt(4));
            arr.add(obj);
        }
        JsonObject obj = new JsonObject();
        obj.add("products", arr);
        return obj.getAsString();
    }
}

