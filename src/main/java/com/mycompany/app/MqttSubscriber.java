package com.mycompany.app;

import java.util.Random;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.eclipse.paho.client.mqttv3.*;
import java.util.concurrent.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MqttSubscriber {

    static class Worker implements Runnable {
        private BlockingQueue<String> queue;
        private int hashIterTime = 1000;

        public Worker(BlockingQueue<String> queue, int hashIterTime) {
            this.queue = queue;
            this.hashIterTime = hashIterTime;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String message = queue.take();
                    for (int i = 0; i < hashIterTime; i++) {
                        sha256(message);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static String getEnv(String envVar, String defaultValue) {
        String value = System.getenv(envVar);
        return (value != null) ? value : defaultValue;
    }

    private static MqttConnectOptions setUpConnectionOptions(String username, String password) {
       MqttConnectOptions connOpts = new MqttConnectOptions();
       connOpts.setCleanSession(true);
       connOpts.setUserName(username);
       connOpts.setPassword(password.toCharArray());
       return connOpts;
    }

    public static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(MqttSubscriber.class);
    public static void main(String[] args) {
        String broker = getEnv("MQTT_BROKER", "tcp://localhost:1883");
        String clientId = getEnv("MQTT_CLIENT_ID", "c-abc");
        String subTopic = getEnv("MQTT_SUB_TOPIC", "+/Lydia/+/State");
        //String pubTopic = getEnv("MQTT_PUB_TOPIC", "APP-03624/Lydia/HIK0189/Order");
        String username = getEnv("MQTT_USERNAME", "user");
        String password = getEnv("MQTT_PASSWORD", "parssword");
        String nThreads = getEnv("N_THTREADS", "500");
        String hashIter = getEnv("N_HASH_ITER", "1000");
        int numberOfThreads = Integer.parseInt(nThreads);
        int hashIterTime = Integer.parseInt(hashIter);
        System.out.println("creating " + numberOfThreads + " threads with " + hashIterTime + " hash iterations");
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        //BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        // 创建并启动 50 个线程
        // for (int i = 0; i < numberOfThreads; i++) {
        //     new Thread(new Worker(queue, hashIterTime), "Worker-" + i).start();
        // }

        try {
            MqttClient client = new MqttClient(broker, clientId);
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String subTopic, MqttMessage message) throws Exception {
                    // 提交任务给线程池执行
                    // try {
                    //     queue.put(message.toString());
                    // } catch (InterruptedException e) {
                    //     Thread.currentThread().interrupt();
                    //     e.printStackTrace();
                    // }
                    executorService.submit(() -> {
                        try {
                            Random random = new java.util.Random();
                            int randomNumber = random.nextInt(165);
                            String index = UUID.randomUUID().toString().replace("-", "");
                            MDC.put("key", index);
                            String payload = new String(message.getPayload(), "utf-8");
                            //logger.info("log in worker thread, message arrived, subTopic = {}, json-msg = {}", subTopic, payload);
                            // 模拟任务处理时间
                            for (int i = 0; i < hashIterTime; i++) {
                                sha256(message.toString());
                            }
                            client.publish("APP-03624/Lydia/" + randomNumber + "/Order", message);
                        } catch (Exception e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // 不用于订阅者
                }
            });

            MqttConnectOptions connOpts = setUpConnectionOptions(username, password);
            client.connect(connOpts);
            client.subscribe(subTopic);

            System.out.println("subscribed subTopic: " + subTopic);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
