package netty.test.util;

public class Util {
    public static void print(String fmt, Object... args) {
        String fmt1 = String.format("[%s] %s\n", Thread.currentThread().getName(), fmt);
        System.out.printf(fmt1, args);
    }

    public static void handleError(Exception e) {
        System.out.println("Error!");
        e.printStackTrace();
    }
}
