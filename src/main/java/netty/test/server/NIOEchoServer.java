package netty.test.server;

import static java.net.StandardSocketOptions.SO_REUSEADDR;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NIOEchoServer {

    public static void main(String[] args) {
        int port = 9090;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        NIOEchoServer server = new NIOEchoServer();
        server.run(port);
    }

    private void run(int port) {
        System.out.println("NIO Echo Server started!");
        try {

            var channel = ServerSocketChannel.open();
            channel.setOption(SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(port));
            channel.configureBlocking(false);

            var selector = Selector.open();
            final var key_a = channel.register(selector, SelectionKey.OP_ACCEPT);
            while (true) {
                System.out.println("SSS call select()");
                selector.select();
                Set<SelectionKey> keys = selector.selectedKeys();
                for (var key : keys) {
                    System.out.println("SSSSS key.interestOps=" + key.interestOps());
                    if (key.isAcceptable()) {
                        System.out.println("XXXXXX isAcceptable");
                        if (key == key_a) {
                            System.out.println("SSAAAME");
                        } else {
                            System.out.println("NNOOOOOOOOO");
                        }
                        handleAccept(key, selector);
                    }
                    if (key.isReadable()) {
                        System.out.println("XXXXXX isReadable");
                        handleRead(key);
                    }
                    if (key.isWritable()) {
                        System.out.println("XXXXXX isWritable");
                        handleWrite(key);
                    }
                }
            }

        } catch (IOException ioe) {
            System.out.println("Error!");
            ioe.printStackTrace();
        }

        System.out.println("Bye!");
    }

    private void handleAccept(SelectionKey key, Selector selector) {
        var serverSocketChannel = (ServerSocketChannel) key.channel();
        try {
            var ch = serverSocketChannel.accept();
            if (ch == null) {
                System.out.println("XXX accept() returned null!");
                return;
            }
            System.out.println("XXX accepted connection!");
            ch.configureBlocking(false);
            ch.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(1024));
        } catch (IOException ioe) {
            System.out.println("Error in accept");
            ioe.printStackTrace();
        }
    }

    private void handleRead(SelectionKey key) {
        if ((key.interestOps() & SelectionKey.OP_READ) == 0) {
            return;
        }
        var ch = (SocketChannel) key.channel();
        var buf = (ByteBuffer) key.attachment();
        try {
            int size = ch.read(buf);
            if (size < 0) {
                // closed
                System.out.println("Connection has ben closed!");
                ch.close();
                return;
            }
            System.out.println("RRRRR size = " + size);
            var bytes = new byte[size];
            buf.flip().get(bytes);
            buf.clear();
            buf.put(processMessage(bytes));
            key.interestOps(SelectionKey.OP_WRITE);
        } catch (IOException ioe) {
            handleError(ioe);
        }3
    }

    private void handleWrite(SelectionKey key) {
        var ch = (SocketChannel) key.channel();
        var buf = ((ByteBuffer) key.attachment()).flip();
        try {
            ch.write(buf);
            buf.compact();
            if (buf.position() > 0) {
                // Data is remaining in buf.
                key.interestOps(SelectionKey.OP_WRITE);
            } else {
                buf.clear();
                key.interestOps(SelectionKey.OP_READ);
            }
        } catch (IOException ioe) {
            handleError(ioe);
        }
    }

    private byte[] processMessage(byte[] bytes) {
        String s = new String(bytes, StandardCharsets.UTF_8);
        if (s.charAt(s.length() - 1) == '\n') {
            s = s.substring(0, s.length() - 1);
        }
        System.out.println("PPPPPPP s = " + s);
        String t = s + " is nio\n";
        return t.getBytes(StandardCharsets.UTF_8);
    }

    private BufferedReader getReader(InputStream is) {
        return new BufferedReader(new InputStreamReader(is));
    }

    private PrintWriter getWriter(OutputStream os) {
        return new PrintWriter(os);
    }

    private void handleError(Exception e) {
        System.out.println("Error!");
        e.printStackTrace();
    }

}
