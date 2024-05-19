import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;


public class ZooKeeperClient {
    private static final String ZOOKEEPER_HOST = "localhost:2181";
    private static ZooKeeper zooKeeper;
    static ArrayList<String> arr = new ArrayList<>();
    static ZooKeeperClient zoo;
    HttpServer server = null;

    public ZooKeeperClient() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_HOST, 3000, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.Expired) {
                try {
                    reconnect();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void reconnect() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_HOST, 3000, null);
    }


    public static void main(String[] args) {
        try {
            zoo=new ZooKeeperClient();
            zoo.startAPIs();
            zoo.watchServers();

        } catch (IOException | InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }

    }

    public void startAPIs(){

        try {
            server = HttpServer.create(new InetSocketAddress(8085), 0);
            server.createContext("/api/system-info", new SystemInfoHandler());
            server.createContext("/api/best-server", new BestServerHandler());
            server.setExecutor(null); // creates a default executor
            server.start();
            System.out.println("Server is running on http://localhost:8085");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public  void registerServer(String path, String ip, String load) throws KeeperException, InterruptedException {
        String data = ip + ":" + load;
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("criado: "+ip+" | CPU: "+load);
        } else {
            zooKeeper.setData(path, data.getBytes(), stat.getVersion());
            System.out.println("Atualizado: "+ip+" | CPU: "+load);
        }
    }

    static class SystemInfoHandler implements HttpHandler  {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // Read request body
                InputStream inputStream = exchange.getRequestBody();
                String requestBody = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

                // Parse JSON request body
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> systemInfo = objectMapper.readValue(requestBody, Map.class);
                String ipAddress = systemInfo.get("ipAddress").toString();
                String cpuUsage = systemInfo.get("cpuUsage").toString();
                String newEntry = ipAddress + ":" + cpuUsage;
                boolean ipAddressExists = false;

                for (int i = 0; i < arr.size(); i++) {
                    if (arr.get(i).startsWith(ipAddress + ":")) {
                        arr.set(i, newEntry);
                        ipAddressExists = true;
                        break;
                    }
                }

                if (!ipAddressExists) {
                    arr.add(newEntry);

                }


                for(int i=0;i<arr.size();i++){
                    try {
                        zoo.registerServer("/servers/"+ipAddress,ipAddress,cpuUsage);
                     } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }


                // Prepare and send response
                String response = objectMapper.writeValueAsString(Map.of("success", true, "message", "System info received successfully"));
                exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);
                OutputStream outputStream = exchange.getResponseBody();
                outputStream.write(response.getBytes(StandardCharsets.UTF_8));
                outputStream.close();
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    static class BestServerHandler implements HttpHandler  {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            if ("GET".equals(exchange.getRequestMethod())) {

                // Parse JSON request body
                ObjectMapper objectMapper = new ObjectMapper();

                String bestIp;
                try {
                     bestIp=zoo.selectBestServer();
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }


                // Prepare and send response
                String response = objectMapper.writeValueAsString(Map.of("bestIp", bestIp, "message", "System info received successfully"));
                exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);
                OutputStream outputStream = exchange.getResponseBody();
                outputStream.write(response.getBytes(StandardCharsets.UTF_8));
                outputStream.close();
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }


    public String selectBestServer() throws KeeperException, InterruptedException {

        List<String> servers = zooKeeper.getChildren("/servers", false);
        String bestServer = null;
        Float minLoad = Float.MAX_VALUE;
        String bestServerIp = null;

        for (String server0 : servers) {
            byte[] serverData = zooKeeper.getData("/servers/" + server0, false, null);
            String[] dataParts = new String(serverData).split(":");
            String ip = dataParts[0];
            Float load = Float.parseFloat(dataParts[1]);

            if (load < minLoad) {
                minLoad = load;
                bestServer = server0;
                bestServerIp = ip;
            }
        }

        return bestServerIp;
    }

    public void updateServerLoad(String path, String ip, int load) throws KeeperException, InterruptedException {
        String data = ip + ":" + load;
        Stat stat = zooKeeper.exists(path, false);
        if (stat != null) {
            zooKeeper.setData(path, data.getBytes(), stat.getVersion());
        }
    }

    public void watchServers() throws KeeperException, InterruptedException {
        zooKeeper.getChildren("/servers", event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    List<String> servers = zooKeeper.getChildren("/servers", false);
                    System.out.println("ZNODES EXISTENTES: "+servers);

                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
}