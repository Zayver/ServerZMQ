package com.distribuidos;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.zeromq.*;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

import java.sql.*;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
    private final static int HEARTBEAT_LIVENESS = 3;
    private final static int HEARTBEAT_INTERVAL = 1000;
    private final static int INTERVAL_INIT = 1000;
    private final static int INTERVAL_MAX = 32000;

    private final static String PPP_READY = "\u0001";
    private final static String PPP_HEARTBEAT = "\u0002";

    private final Connection con;
    private final Logger logger = Logger.getLogger("Server");

    Server() throws SQLException {
        con = DriverManager.getConnection("jdbc:mysql://db-mysql-nyc1-26039-do-user-12425800-0.b.db.ondigitalocean.com:25060/defaultdb", "doadmin", "AVNS_xSBmY-Vlbi_ADXbmXmT");
    }


    private Socket worker_socket(ZContext ctx) {
        Socket worker = ctx.createSocket(SocketType.DEALER);
        worker.connect("tcp://172.22.250.122:5556");

        //  Tell queue we're ready for work
        logger.log(Level.INFO, "Server ready for listening connections");
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
                        logger.log(Level.INFO, "Received query");
                        var result = operation(msg.getLast().toString());
                        msg.getLast().reset(result);
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
                            logger.log(Level.SEVERE, "Invalid message");
                            msg.dump(System.out);
                        }
                        msg.destroy();
                    } else {
                        logger.log(Level.SEVERE, "Invalid message");
                        msg.dump(System.out);
                    }
                    interval = INTERVAL_INIT;
                } else if (--liveness == 0) {
                    logger.log(Level.WARNING, "Check health failed, cannot get to LoadBalancer");
                    logger.log(Level.WARNING, "reconnecting in "+interval+" msec");

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

                if (System.currentTimeMillis() > heartbeat_at) {
                    long now = System.currentTimeMillis();
                    heartbeat_at = now + HEARTBEAT_INTERVAL;
                    logger.log(Level.INFO, "Server still alive");
                    ZFrame frame = new ZFrame(PPP_HEARTBEAT);
                    frame.send(worker, 0);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private String operation(String data) throws SQLException {
        System.out.println("Test: " + data);
        JsonObject query =  new Gson().fromJson(data, JsonObject.class);
        String operacion = query.get("operation").getAsString();

        if(Objects.equals(operacion, "consulta")){
            return getAllProducts();
        }
        else if (Objects.equals(operacion, "signup")){
            return signUp(data);
        } else if (Objects.equals(operacion, "login")) {
            return logIn(data);
        }
        else if(Objects.equals(operacion, "buy")){
            return buyProduct(data);
        }
        return "{result = false, body = [\"Op no definida\"]}";
    }


    private String getAllProducts() throws SQLException {
        String sql = "select id, name, price, amount, description from product";
        Statement stat = con.createStatement();
        var rs = stat.executeQuery(sql);
        JsonArray arr = new JsonArray();
        while (rs.next()) {
            var obj = new JsonObject();
            obj.addProperty("id", rs.getInt(1));
            obj.addProperty("nombre", rs.getString(2));
            obj.addProperty("precio", rs.getDouble(3));
            obj.addProperty("amount", rs.getInt(4));
            obj.addProperty("descripcion", rs.getString(5));
            arr.add(obj);
        }
        JsonObject obj = new JsonObject();
        obj.addProperty("result", true);
        obj.add("body", arr);
        return obj.toString();
    }


    private String signUp(String data) throws SQLException {
        JsonObject query = new Gson().fromJson(data, JsonObject.class);
        JsonArray mainObj = query.getAsJsonArray("params");
        String username = mainObj.get(0).getAsJsonObject().get("user").getAsString();
        String password = mainObj.get(0).getAsJsonObject().get("user").getAsString();;
        String sql = "insert into usersd (user_name, password) values (?,?);";
        PreparedStatement pstat = con.prepareStatement(sql);
        pstat.setString(1, username);
        pstat.setString(2, password);
        pstat.executeUpdate();

        JsonObject result = new JsonObject();
        result.addProperty("result", true);
        JsonArray body = new JsonArray();
        body.add("Se ha autenticado correctamente");
        result.add("body", body);
        return result.toString();
    }

    private String logIn(String data) throws SQLException {
        JsonObject query = new Gson().fromJson(data, JsonObject.class);
        JsonArray mainObj = query.getAsJsonArray("params");
        String username = mainObj.get(0).getAsJsonObject().get("user").getAsString();
        String password = mainObj.get(0).getAsJsonObject().get("password").getAsString();

        String sql = "select id from usersd where user_name = ? and password = ?;";
        PreparedStatement pstat = con.prepareStatement(sql);
        pstat.setString(1, username);
        pstat.setString(2, password);
        var rs = pstat.executeQuery();

        if(rs.next()){
            JsonObject obj = new JsonObject();
            obj.addProperty("result", true);
            JsonArray body = new JsonArray();
            body.add("Se ha autenticado correctamente");
            obj.add("body", body);
            return obj.toString();
        }
        JsonObject obj = new JsonObject();
        obj.addProperty("result", false);
        JsonArray body = new JsonArray();
        body.add("Error, usuario o contraseña inválidos");
        obj.add("body", body);
        return obj.toString();
    }
    private String buyProduct(String data) throws SQLException {
        JsonObject query = new Gson().fromJson(data, JsonObject.class);
        JsonArray mainObj = query.getAsJsonArray("body");
        int id = mainObj.get(0).getAsJsonObject().get("id").getAsInt();
        int quantity = mainObj.get(0).getAsJsonObject().get("amount").getAsInt();

        String sql = "select id, amount from product where id = ? ;";
        PreparedStatement pstat = con.prepareStatement(sql);
        pstat.setInt(1, id);
        var rs = pstat.executeQuery();
        rs.next();
        int productQuantity = rs.getInt(2);
        if(productQuantity >= quantity){
            String sqlUpdate = "update product set amount ? where id = ?;";
            PreparedStatement innetPstat = con.prepareStatement(sqlUpdate);
            innetPstat.setInt(1, productQuantity-quantity);
            innetPstat.setInt(2, id);
            innetPstat.executeUpdate();
        }

        JsonObject obj = new JsonObject();
        obj.addProperty("result", productQuantity >= quantity);
        JsonArray arr = new JsonArray();
        if(productQuantity >= quantity){
            arr.add("Comprados todos los productos");
        }
        else{
            arr.add("No hay cantidades disponibles");
        }
        obj.add("body", arr);
        return obj.toString();
    }


}

