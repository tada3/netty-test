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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NIOSequentialEchoClient implements Runnable {

    private static final int BUF_SIZE = 10;
    private static final byte[] QUIT = { (byte) 0xCC, (byte) 0xDD, (byte) 0xEE, (byte) 0xFF };
    private static final byte[] DEL = { (byte) 0xAB, (byte) 0xBC, (byte) 0xCD, (byte) 0xDE };

    private ByteBuffer buf;
    private Selector selector;
    private volatile boolean stopped;
    private Queue<Object> queue = new ArrayDeque<>();
    private SocketAddress addr;
    private StringBuilder sb;
    private DataSource ds;
    private long processedQuery;
    private int id;

    public static void main(String[] args) {
        String host = "localhost";
        int port = 9090;
        int maxRequests = 0;
        long testPeriod = 3 * 1000;
        int numOfClient = 3;

        if (args.length > 0) {
            host = args[0];
        }
        if (args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            maxRequests = Integer.parseInt(args[2]);
        }
        if (args.length > 3) {
            testPeriod = Long.parseLong(args[3]);
        }
        if (args.length > 4) {
            numOfClient = Integer.parseInt(args[4]);
        }

        try {
            Thread[] ths = new Thread[numOfClient];
            NIOSequentialEchoClient[] clients = new NIOSequentialEchoClient[numOfClient];
            for (int i = 0; i < numOfClient; i++) {
                System.out.println("Client " + i + " started!");
                clients[i] = new NIOSequentialEchoClient(i, host, port, maxRequests);
                ths[i] = new Thread(clients[i]);
            }

            for (int i = 0; i < numOfClient; i++) {
                ths[i].start();
            }

            Thread.sleep(testPeriod);

            for (int i = 0; i < numOfClient; i++) {
                clients[i].stop();
            }

            for (int i = 0; i < numOfClient; i++) {
                System.out.println("Joining " + i);
                ths[i].join();
            }

            System.out.println("Done.");

            for (int i = 0; i < numOfClient; i++) {
                System.out.println("processedQuery[" + i + "]: " + clients[i].getProcessedQuery());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long getProcessedQuery() {
        return processedQuery;
    }

    public NIOSequentialEchoClient(int id, String host, int port, int n) {
        this.id = id;
        addr = new InetSocketAddress(host, port);
        sb = new StringBuilder();
        ds = new DataSource(queue, n);
    }

    public void stop() {
        System.out.println("Stopping..");
        ds.sendQuit();
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
                //System.out.println("\nCalling select(): " + myKey.interestOps());

                selector.select();
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator iter = keys.iterator();
                while (iter.hasNext()) {
                    SelectionKey key = (SelectionKey) iter.next();
                    iter.remove();

                    //System.out.println("  key: " + key.interestOps() + ", " + key.readyOps());

                    if (key.isConnectable()) {
                        //System.out.println("  Calling handleConnectable()");
                        handleConnetable(key);
                    }
                    if (key.isWritable()) {
                        //System.out.println("  Calling handleWritable()");
                        handleWritable(key);
                    }
                    if (key.isValid() && key.isReadable()) {
                        //System.out.println("  Calling handleReadable()");
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
            ds.requestMessage(true);
            key.interestOps(SelectionKey.OP_WRITE);
        } catch (IOException ioe) {
            handleError(ioe);
        }
    }

    private void handleReadable(SelectionKey key) {
        var ch = (SocketChannel) key.channel();
        try {
            //System.out.println("  read: reading from ch..");

            boolean readSomething = false;
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
                    //System.out.println("  read: no more data!");
                    if (readSomething) {
                        ds.requestMessage(false);
                        key.interestOps(SelectionKey.OP_WRITE);
                    }
                    return;
                }

                //System.out.println("  read: size = " + count);
                var bytes = new byte[count];
                buf.flip().get(bytes);
                buf.clear();

                long finished = printMessage(bytes);
                processedQuery += finished;
                readSomething = true;
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
                        //System.out.println("  write: no data found");
                        return;
                    }
                    if (Arrays.compare(dataOut, QUIT) == 0) {
                        System.out.println("  write: quit");
                        ch.close();
                        stop();
                        return;
                    }
                    if (Arrays.compare(dataOut, DEL) == 0) {
                        //System.out.println(" write: done");
                        key.interestOps(SelectionKey.OP_READ);
                        return;
                    }

                    buf.put(dataOut);
                }
                buf.flip();
                //System.out.println("  write: writing to ch..");
                int writeCount = ch.write(buf);
                //System.out.println("  write: writeCount = " + writeCount);
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

    private long printMessage(byte[] data) {
        String s = new String(data, StandardCharsets.UTF_8);
        //System.out.println("  pm: Receieved: " + s);

        if (s.equals("\n")) {
            sb.append(s);
            //System.out.print("res: " + sb.toString());
            sb.setLength(0);
            return 1;
        }

        long count = 0;

        String[] ss = s.split("\n");
        for (int i = 0; i < ss.length; i++) {
            if (i == ss.length - 1) {
                if (s.endsWith("\n")) {
                    sb.append(ss[i]);
                    //System.out.println("res: " + sb.toString());
                    sb.setLength(0);
                    count++;
                } else {
                    sb.append(ss[i]);
                }
            } else {
                sb.append(ss[i]);
                //System.out.println("res: " + sb.toString());
                sb.setLength(0);
                count++;
            }
        }
        return count;
    }

    private void handleError(Exception e) {
        System.out.println("Error!");
        e.printStackTrace();
    }

    private static class DataSource {
        private static final long WAIT_TIME = 10; // 10 ms
        private int maxMsgs;
        private int count;
        private Queue<Object> queue;
        private String[] words = {
                "CategroyTheory", "Monoid", "AdjointFunctions", "ProductsAndCoProducts",
                "CategoriesAndFunctionsWithoutAdmittingIt", "X68000IsGreatPersonalComputer!!!"
        };
        private Random rnd = new Random();
        private ExecutorService myEs = Executors.newSingleThreadExecutor();

        private BlockingQueue<String> internalQueue = new ArrayBlockingQueue<>(10);

        public DataSource(Queue<Object> queue, int n) {
            this.queue = queue;
            maxMsgs = n;
            myEs.submit(() -> {

                while (true) {
                    String msg = internalQueue.take();
                    if (msg == null) {
                        System.out.println("No msg!!!");
                        continue;
                    }
                    // Wait for a while
                    Thread.sleep(WAIT_TIME);
                    sendMessage(msg);
                }
            });
        }

        public void requestMessage(boolean direct) {
            if (maxMsgs > 0 && count >= maxMsgs) {
                sendQuit();
                return;
            }

            String msg = genString();
            //System.out.println("msg: " + msg);
            if (direct) {
                sendMessage(msg);
            } else {
                boolean added = internalQueue.offer(msg);

                if (!added) {
                    System.out.println("ERROR!");
                    throw new IllegalStateException("Failed to put Msg to internalQ!");
                }
            }
            count++;
        }

        private String genString() {
            int wordIdx = rnd.nextInt(words.length);
            String msg = words[wordIdx] + "-" + rnd.nextInt();
            return msg;
        }

        private void sendMessage(String m) {
            //System.out.println("msg2: " + m);
            String m1 = m + "\n";
            byte[] bytesOut = m1.getBytes(StandardCharsets.UTF_8);
            for (int pos = 0; pos < bytesOut.length; ) {

                int rem = bytesOut.length - pos;
                int cutSize = Math.min(BUF_SIZE, rem);
                byte[] dataOut = new byte[cutSize];
                System.arraycopy(bytesOut, pos, dataOut, 0, cutSize);
                boolean added = queue.offer(dataOut);
                if (!added) {
                    throw new IllegalStateException("Failed to put msg to Q!");
                }
                pos += cutSize;
            }
            queue.offer(DEL);
        }

        public void sendQuit() {
            System.out.println("  ds: send quit");
            boolean added = queue.offer(QUIT);
            if (!added) {
                throw new IllegalStateException("Failed to put msg to Q!");
            }
        }
    }

}
