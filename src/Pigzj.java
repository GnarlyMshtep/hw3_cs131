import java.io.IOException;

public class Pigzj {
    public static void main(String[] args) throws IOException {
        MultiThreadedPigzjCompressor cmp = new MultiThreadedPigzjCompressor(args);
        System.err.println("hello!");
        try {
            cmp.multiThreadCompressStdinToStdout();

        } catch (IOException ex) {
            System.err.println("Threw " + ex + " probably the result of no directed input stream?");
        } catch (Throwable ex) {
            System.err.println("Threw " + ex);

        }
    }
}