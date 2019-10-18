package netty.test.server;

import static java.net.StandardSocketOptions.SO_REUSEADDR;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class NIOEchoServer {

    private static final int BUF_SIZE = 100;

    private final Map<String, ArrayDeque<Object>> appData = new HashMap<>();
    private final Map<String, ClientStatus> clientStatusMap = new HashMap<>();

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
                //System.out.println("\nCalling select()");
                selector.select();
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator iter = keys.iterator();
                while (iter.hasNext()) {
                    SelectionKey key = (SelectionKey) iter.next();
                    iter.remove();

                    //System.out.println("  key: " + key.interestOps() + ", " + key.readyOps());
                    if (key.isAcceptable()) {
                        //System.out.println("  Calling handleAcceptable()");
                        handleAcceptable(key, selector);
                    } else {
                        if (key.isReadable()) {
                            //System.out.println("  Calling handleReadable()");
                            handleReadable(key);
                        }
                        if (key.isValid() && key.isWritable()) {
                            //System.out.println("  Calling handleWritable()");
                            handleWritable(key);
                        }
                    }
                }
            }

        } catch (IOException ioe) {
            System.out.println("Error!");
            ioe.printStackTrace();
        }

        System.out.println("Bye!");
    }

    private void handleAcceptable(SelectionKey key, Selector selector) {
        var serverSocketChannel = (ServerSocketChannel) key.channel();
        try {
            var ch = serverSocketChannel.accept();
            if (ch == null) {
                System.out.println("  accept: no connection is available.");
                return;
            }

            System.out.println("  accept: connection: " + ch.getRemoteAddress());
            ch.configureBlocking(false);
            ch.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(BUF_SIZE));
        } catch (IOException ioe) {
            handleError(ioe);
        }
    }

    private void handleReadable(SelectionKey key) {
        var ch = (SocketChannel) key.channel();
        var buf = (ByteBuffer) key.attachment();
        try {
           // System.out.println("  read: reading from ch..");
            int total = 0;
            int count;

            while (true) {
                count = ch.read(buf);
                if (count <= 0) {
                    if (count < 0) {
                        // closed
                        System.out.println("  read: connection has been closed!");
                        ch.close();
                        return;
                    }
                    //System.out.println("  read: no more data!");
                    if (total > 0) {
                        key.interestOps(SelectionKey.OP_WRITE);
                    }
                    return;
                }

                //System.out.println("  read: size = " + count);
                var bytes = new byte[count];
                buf.flip().get(bytes);
                buf.clear();

                processMessage(bytes, ch.getRemoteAddress());
                total += count;
            }
        } catch (IOException ioe) {
            handleError(ioe);
        }
    }

    private void handleWritable(SelectionKey key) {
        var ch = (SocketChannel) key.channel();
        var buf = ((ByteBuffer) key.attachment());
        try {
            var queue = appData.get(ch.getRemoteAddress().toString());

            while (true) {
                if (buf.position() == 0) {
                    byte[] dataOut = (byte[]) queue.poll();
                    if (dataOut == null) {
                        // No data
                        key.interestOps(SelectionKey.OP_READ);
                        return;
                    }
                    
                    buf.put(dataOut);
                }
                buf.flip();
                //System.out.println("  write: writing to ch..");
                int writeCount = ch.write(buf);
                //System.out.println("  write: writeCount=" + writeCount);
                buf.compact();
                if (buf.position() > 0) {
                    // Data is remaining in buf.
                    System.out.println("  write: data is remaining in buf.");
                    key.interestOps(SelectionKey.OP_WRITE);
                    return;
                } else {
                    buf.clear();
                }
            }
        } catch (IOException ioe) {
            handleError(ioe);
        }
    }

    private void processMessage(byte[] bytesIn, SocketAddress addr) {
        Queue<Object> queue = getQueue(addr);
        ClientStatus clientStatus = getStatus(addr);
        String s = new String(bytesIn, StandardCharsets.UTF_8);
        //System.out.println("  Processing data: " + s + ", " + clientStatus);

        if (s.equals("\n") && clientStatus == ClientStatus.WRITING) {
            putResponseToQueue(" is nio!\n", queue);
            updateStatus(addr, ClientStatus.DONE);
            return;
        }

        boolean endWithNewline = s.endsWith("\n");
       // System.out.println("  pm: endWithNewLine=" + endWithNewline);

        String[] tokens = s.split("\n");
        for (int i = 0; i < tokens.length; i++) {

            String token = tokens[i];
            if (token.isEmpty()) {
                continue;
            }
            if (i < tokens.length - 1 || endWithNewline) {
                token = token + " is nio!\n";
            }
            //System.out.println("  pm: token=" + token);
            putResponseToQueue(token, queue);
        }
        updateStatus(addr, endWithNewline ? ClientStatus.DONE : ClientStatus.WRITING);

    }

    private void putResponseToQueue(String token, Queue<Object> queue) {
        byte[] bytesOut = token.getBytes(StandardCharsets.UTF_8);
        for (int pos = 0; pos < bytesOut.length; ) {
            int rem = bytesOut.length - pos;
            int cutSize = Math.min(BUF_SIZE, rem);
            byte[] dataOut = new byte[cutSize];
            System.arraycopy(bytesOut, pos, dataOut, 0, cutSize);
            queue.offer(dataOut);
            pos += cutSize;
        }
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

    private Queue<Object> getQueue(SocketAddress addr) {
        String key = addr.toString();
        return appData.computeIfAbsent(key, x -> new ArrayDeque<>());
    }

    private enum ClientStatus {
        WRITING,
        DONE;
    }

    private void updateStatus(SocketAddress addr, ClientStatus status) {
        String key = addr.toString();
        clientStatusMap.put(key, status);
    }

    private ClientStatus getStatus(SocketAddress addr) {
        String key = addr.toString();
        return clientStatusMap.get(key);
    }

}
