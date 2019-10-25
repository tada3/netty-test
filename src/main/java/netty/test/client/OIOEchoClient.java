package netty.test.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import netty.test.util.Util;

public class OIOEchoClient implements Runnable {

    private static final long INTERVAL_1 = 10L; // 10 ms
    private static final long INTERVAL_2 = 5L * 1000L; // 5 sec

    private volatile boolean stopped;
    private DataSource ds;
    private String host;
    private int port;
    private int repeatCount;

    /**
     * Usage: cmd <host> <port> <repeat_count> <test_period (sec)> <num_of_clients>
     * @param args
     */
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9090;
        int repeatCount = 10;
        long testPeriod = 10 * 1000L;
        int numOfClient = 3;

        if (args.length > 0) {
            host = args[0];
        }
        if (args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            repeatCount = Integer.parseInt(args[2]);
            Util.print("maxRequests=%d", repeatCount);
        }
        if (args.length > 3) {
            testPeriod = Long.parseLong(args[3]) * 1000L;
        }
        if (args.length > 4) {
            numOfClient = Integer.parseInt(args[4]);
        }

        try {
            Thread[] ths = new Thread[numOfClient];
            OIOEchoClient[] clients = new OIOEchoClient[numOfClient];
            for (int i = 0; i < numOfClient; i++) {
                Util.print("Client %d started!", i);
                clients[i] = new OIOEchoClient(i, host, port, repeatCount);
                ths[i] = new Thread(clients[i]);
                ths[i].setName("T" + i);
            }

            for (int i = 0; i < numOfClient; i++) {
                ths[i].start();
            }

            Thread.sleep(testPeriod);

            for (int i = 0; i < numOfClient; i++) {
                clients[i].stop();
            }

            for (int i = 0; i < numOfClient; i++) {
                Util.print("Joining %d", i);
                ths[i].join();
            }

            Util.print("Done.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public OIOEchoClient(int id, String host, int port, int n) {
        ds = new DataSource();
        this.host = host;
        this.port = port;
        repeatCount = n;
    }

    public void stop() {
        System.out.println("Stopping..");
        stopped = true;
    }

    @Override
    public void run() {
        int iteration = 0;
        try {
            long waitTime = ds.getWaitTime();
            Thread.sleep(waitTime);

            while (!stopped) {
                Util.print("iteration: %d", iteration);
                Socket socket = new Socket(host, port);

                for (int i = 0; i < repeatCount; i++) {
                    String msg = ds.getMessage();
                    OutputStream output = socket.getOutputStream();
                    PrintWriter writer = new PrintWriter(output, true);
                    writer.println(msg);

                    InputStream input = socket.getInputStream();
                    InputStreamReader reader = new InputStreamReader(input);
                    BufferedReader br = new BufferedReader(reader);
                    String res = br.readLine();

                    Util.print("<-- %s", res);
                    Thread.sleep(INTERVAL_1);
                }

                socket.close();

                Util.print("Take a nap!");
                Thread.sleep(INTERVAL_2);
                iteration++;
            }
            Util.print("Stopped.");
        } catch (Exception e) {
            handleError(e);
        }
    }


    private void handleError(Exception e) {
        System.out.println("Error!");
        e.printStackTrace();
    }

    private static class DataSource {
        private String[] words = {
                "CategroyTheory", "Monoid", "AdjointFunctions", "ProductsAndCoProducts",
                "CategoriesAndFunctionsWithoutAdmittingIt", "X68000IsGreatPersonalComputer!!!"
        };

        private Random rnd = new Random();

        public String getMessage() {
            int wordIdx = rnd.nextInt(words.length);
            return words[wordIdx] + "-" + rnd.nextInt();
        }

        public long getWaitTime() {
            return rnd.nextInt((int) INTERVAL_2);
        }
    }

}
