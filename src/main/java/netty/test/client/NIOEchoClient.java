package netty.test.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

public class NIOEchoClient implements Runnable {

    private static final int BUF_SIZE = 10;

    private ByteBuffer buf;
    private Selector selector;
    private volatile boolean stopped;
    private Queue<Object> queue = new ArrayDeque<>();
    private SocketAddress addr;
    private StringBuilder sb;

    public static void main(String[] args) {
        try {
            var client = new NIOEchoClient("localhost", 9090);
            Thread th = new Thread(client);
            th.start();

            System.out.println("Main: sleep 1000");
            Thread.sleep(2);

            client.sendMessage("111");
            client.sendMessage("abcde12345");
            System.out.println("Main: sleep 2000");
            Thread.sleep(4000);
            client.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public NIOEchoClient(String host, int port) {

        addr = new InetSocketAddress(host, port);
        sb = new StringBuilder();
    }

    public void stop() {
        stopped = true;
    }

    public void sendMessage(String m) {

        String m1 = m + "\n";
        byte[] bytesOut = m1.getBytes(StandardCharsets.UTF_8);
        for (int pos = 0; pos < bytesOut.length; ) {

            int rem = bytesOut.length - pos;
            int cutSize = Math.min(BUF_SIZE, rem);
            byte[] dataOut = new byte[cutSize];
            System.arraycopy(bytesOut, pos, dataOut, 0, cutSize);

            boolean added = queue.offer(dataOut);
            if (!added) {
                System.out.println("  Failed to put msg to Q!");
            }
            pos += cutSize;
        }

    }

    private int repeat = 0;

    @Override
    public void run() {
        SocketChannel ch = null;
        try {
            ch = SocketChannel.open();
            ch.configureBlocking(false);

            selector = Selector.open();
            var myKey = ch.register(selector, SelectionKey.OP_CONNECT);
            ch.connect(addr);
            buf = ByteBuffer.allocate(BUF_SIZE);

            while (!stopped) {

                repeat++;
                if (repeat == 5000) {
                    break;
                }

                if (ch.isConnected() && queue.peek() != null) {
                    myKey.interestOps(SelectionKey.OP_WRITE);
                }

                System.out.println("\nCalling select()");

                selector.select(1000);
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator iter = keys.iterator();
                while (iter.hasNext()) {
                    SelectionKey key = (SelectionKey) iter.next();
                    iter.remove();

                    System.out.println("  key: " + key.interestOps() + ", " + key.readyOps());

                    if (key.isConnectable()) {
                        System.out.println("  Calling handleConnectable()");
                        handleConnetable(key);
                    }
                    if (key.isWritable()) {
                        System.out.println("  Calling handleWritable()");
                        handleWritable(key);
                    }
                    if (key.isReadable()) {
                        System.out.println("  Calling handleReadable()");
                        handleReadable(key);
                    }
                }
            }
            System.out.println("Stopped!");
        } catch (Exception e) {
            handleError(e);
        } finally {
            if (ch != null) {
                try {
                    ch.close();
                } catch (IOException ioe) {
                    handleError(ioe);
                }
            }
        }
    }

    private void handleConnetable(SelectionKey key) {
        try {
            var channel = (SocketChannel) key.channel();
            if (channel.isConnectionPending()) {
                channel.finishConnect();
            }
            key.interestOps(SelectionKey.OP_WRITE);
        } catch (IOException ioe) {
            handleError(ioe);
        }
    }

    private void handleReadable(SelectionKey key) {
        var ch = (SocketChannel) key.channel();
        try {
            System.out.println("  read: reading from ch..");

            int count;
            while (true) {
                count = ch.read(buf);
                if (count <= 0) {
                    if (count < 0) {
                        // closed
                        System.out.println("  read: connection has ben closed!");
                        ch.close();
                        return;
                    }
                    System.out.println("  read: no more data!");
                    return;
                }

                System.out.println("  read: size = " + count);
                var bytes = new byte[count];
                buf.flip().get(bytes);
                buf.clear();

                printMessage(bytes);
            }
        } catch (IOException ioe) {
            handleError(ioe);
        }
    }

    private void handleWritable(SelectionKey key) {
        var ch = (SocketChannel) key.channel();
        try {
            while (true) {
                if (buf.position() == 0) {
                    byte[] dataOut = (byte[]) queue.poll();

                    if (dataOut == null) {
                        // No data
                        System.out.println("  write: no data found");
                        key.interestOps(SelectionKey.OP_READ);
                        return;
                    }
                    buf.put(dataOut);
                }
                buf.flip();
                System.out.println("  write: writing to ch..");
                int writeCount = ch.write(buf);
                System.out.println("  write: writeCount = " + writeCount);
                buf.compact();
                if (buf.position() > 0) {
                    // Data is remaining in buf.
                    System.out.println("  write: data is remaining in buf.");
                    key.interestOps(SelectionKey.OP_WRITE);
                    return;
                } else {
                    buf.clear();
                    //key.interestOps(SelectionKey.OP_READ);
                }
            }
        } catch (IOException ioe) {
            handleError(ioe);
        }
    }

    private void printMessage(byte[] data) {
        String s = new String(data, StandardCharsets.UTF_8);
        System.out.println("  pm: Receieved: " + s);

        if (s.equals("\n")) {
            sb.append(s);
            System.out.println("Reply: " + sb.toString());
            sb.setLength(0);
            return;
        }

        String[] ss = s.split("\n");
        for (int i = 0; i < ss.length; i++) {
            if (i == ss.length - 1) {
                if (s.endsWith("\n")) {
                    sb.append(ss[i]);
                    System.out.println("Reply: " + sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(ss[i]);
                }
            } else {
                sb.append(ss[i]);
                System.out.println("Reply: " + sb.toString());
                sb.setLength(0);
            }
        }
    }

    private void handleError(Exception e) {
        System.out.println("Error!");
        e.printStackTrace();
    }

}
