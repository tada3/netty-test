package netty.test.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import netty.test.util.BatchLogger;
import netty.test.util.Util;

public class OIOEchoServer {
    private static final int THREAD_POOL_SIZE = 5;
    private static Thread mainThread;
    private ServerSocket serverSocket;
    private volatile boolean stopped = false;
    private final BatchLogger logger = new BatchLogger();
    private final ExecutorService es = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static void main(String[] args) {
        mainThread = Thread.currentThread();
        final OIOEchoServer server = new OIOEchoServer();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Util.print("Stopping server..");
                server.stopServer();
            }
        });

        int port = 9090;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }

        try {
            server.run(port);
        } catch (Exception e) {
            Util.handleError(e);
        } finally {
            Util.print("Bye!");
        }
    }

    private void stopServer() {
        stopped = true;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ioe) {
            Util.handleError(ioe);
        }
    }

    private void run(int port) {
        System.out.println("OIO Echo Server started!");
        try {
            serverSocket = new ServerSocket(port);
            while (!stopped) {
                Socket s = serverSocket.accept();
                es.submit(() -> {
                    try {
                        var reader = getReader(s.getInputStream());
                        var writer = getWriter(s.getOutputStream());
                        long t1 = System.currentTimeMillis();
                        long count = 0;
                        String line;
                        while ((line = reader.readLine()) != null) {
                            //System.out.println("Threads: " + Thread.activeCount());
                            //System.out.println("read: " + line);
                            writer.println(line + " is oio!");
                            writer.flush();
                            count++;
                        }
                        logger.log(t1, count);
                    } catch (Exception ioe) {
                        System.out.println("Errro1!");
                        ioe.printStackTrace();
                    }
                });
            }
        } catch (Exception ioe) {
            Util.print("Error2! " + ioe);
            ioe.printStackTrace();
        } finally {
            close();
        }


    }

    private void close() {
        es.shutdown();
        try {
            es.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Util.handleError(e);
            Thread.currentThread().interrupt();
        } finally{
            es.shutdownNow();
        }
        logger.close();
    }

    private BufferedReader getReader(InputStream is) {
        return new BufferedReader(new InputStreamReader(is));
    }

    private PrintWriter getWriter(OutputStream os) {
        return new PrintWriter(os);
    }

}
