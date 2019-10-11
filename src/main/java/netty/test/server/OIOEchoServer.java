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

public class OIOEchoServer {

    public static void main(String[] args) {
        int port = 9090;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        OIOEchoServer server = new OIOEchoServer();
        server.run(port);
    }

    private void run(int port) {
        System.out.println("OIO Echo Server started!");
        ExecutorService es = Executors.newFixedThreadPool(10);

        try {
            var serverSocket = new ServerSocket(port);
            while (true) {
                Socket s = serverSocket.accept();
                es.submit(() -> {
                    try {
                        var reader = getReader(s.getInputStream());
                        var writer = getWriter(s.getOutputStream());
                        String line;
                        while ((line = reader.readLine()) != null) {
                            System.out.println("Threads: " + Thread.activeCount());
                            System.out.println("read: " + line);
                            writer.println(line + " is cool");
                            writer.flush();
                        }
                    } catch (IOException ioe) {
                        System.out.println("Errro1!");
                        ioe.printStackTrace();
                    }
                });
            }
        } catch (IOException ioe) {
            System.out.println("Error2!");
            ioe.printStackTrace();
        }

        System.out.println("Bye!");
    }

    private BufferedReader getReader(InputStream is) {
        return new BufferedReader(new InputStreamReader(is));
    }

    private PrintWriter getWriter(OutputStream os) {
        return new PrintWriter(os);
    }

}
