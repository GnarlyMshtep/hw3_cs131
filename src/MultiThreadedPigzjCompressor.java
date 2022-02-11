import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.*;

public class MultiThreadedPigzjCompressor {
    // finals
    public final static int BLOCK_SIZE = 131072;
    public final static int DICT_SIZE = 32768;
    private final static int GZIP_MAGIC = 0x8b1f;
    private final static int TRAILER_SIZE = 8; // !this should prob be used
    private final static int NUM_AVALBILE_CORES = Runtime.getRuntime().availableProcessors();

    public ByteArrayOutputStream outStream;
    private CRC32 crc = new CRC32();
    private int numThreads;

    private ThreadPoolExecutor compressExecutor;
    private ConcurrentHashMap<Integer, CompressedBlock> blockStorage;

    MultiThreadedPigzjCompressor(String[] args) {
        this.numThreads = NUM_AVALBILE_CORES;
        // get number of expected processes

        int retFromUserOptions = handleUserOptions(args);
        if (retFromUserOptions >= 1) {
            this.numThreads = retFromUserOptions;
        }
        System.err.println("we will be working with " + this.numThreads + " threads");
        // init objects
        this.compressExecutor = new ThreadPoolExecutor(this.numThreads, this.numThreads, 60L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>());
        this.blockStorage = new ConcurrentHashMap<Integer, CompressedBlock>();
        this.outStream = new ByteArrayOutputStream();
    }

    public void multiThreadCompressStdinToStdout() throws IOException, InterruptedException {
        this.writeHeader(); // write the header, it always goes first, so it's fine
        this.crc.reset(); // we reset the crc

        byte[] blockBuf = new byte[BLOCK_SIZE];
        byte[] dictBuf = new byte[DICT_SIZE];
        boolean hasDict = false;

        long totalBytesRead = 0;
        int serialNumCount = 0;

        int nBytes = efficientGetNextBlock(blockBuf);
        totalBytesRead += nBytes;

        while (nBytes > 0) {
            crc.update(blockBuf, 0, nBytes);

            byte[] deepCopyBuffer = blockBuf.clone();
            byte[] deepCopyDict = dictBuf.clone();
            compressExecutor.execute(
                    new CompressWorkerThread(serialNumCount, blockStorage, deepCopyBuffer, nBytes, deepCopyDict,
                            hasDict,
                            (nBytes < BLOCK_SIZE || System.in.available() <= 0)));

            if (nBytes >= DICT_SIZE) {// set up a dict if possible
                System.arraycopy(blockBuf, nBytes - DICT_SIZE, dictBuf, 0, DICT_SIZE);
                hasDict = true;
            } else {
                hasDict = false;
            }

            serialNumCount++;
            nBytes = getNextBlock(blockBuf);
            totalBytesRead += nBytes;
        }
        compressExecutor.shutdown();
        compressExecutor.awaitTermination(60L, TimeUnit.MINUTES);
        for (int i = 0; i < serialNumCount; i++) {
            outStream.write(blockStorage.get(i).getBlock(), 0, blockStorage.get(i).getBlockLength());
        }
        System.err.println("I read " + totalBytesRead + " and will write out " + outStream.size() + " bytes");

        byte[] trailerBuf = new byte[TRAILER_SIZE];
        writeTrailer(totalBytesRead, trailerBuf, 0);
        outStream.write(trailerBuf);
        outStream.writeTo(System.out);
        System.err.println(outStream.size());
    }

    private static int efficientGetNextBlock(byte[] blockBuf) throws IOException {
        if (System.in.available() > 0) {
            int numBytesRead = System.in.read(blockBuf);
            return numBytesRead;
        } else {
            return -1;
        }

    }

    /**
     * really slow, optmize by sytem.read and put in blockBuf
     * 
     * @param blockBuf
     * @return
     * @throws IOException
     */
    private static int getNextBlock(byte[] blockBuf) throws IOException {
        int b;
        int i = 0;

        while (i < blockBuf.length) {
            b = System.in.read();
            if (b == -1)
                break;
            blockBuf[i] = (byte) b;
            i++;
        }
        return i;
    }

    private void writeHeader() throws IOException {
        outStream.write(new byte[] {
                (byte) GZIP_MAGIC, // Magic number (short)
                (byte) (GZIP_MAGIC >> 8), // Magic number (short)
                Deflater.DEFLATED, // Compression method (CM)
                0, // Flags (FLG)
                0, // Modification time MTIME (int)
                0, // Modification time MTIME (int)
                0, // Modification time MTIME (int)
                0, // Modification time MTIME (int)Sfil
                0, // Extra flags (XFLG)
                0 // Operating system (OS)
        });
    }

    private void writeTrailer(long totalBytes, byte[] buf, int offset)
            throws IOException {
        writeInt((int) crc.getValue(), buf, offset); // CRC-32 of uncompr. data
        writeInt((int) totalBytes, buf, offset + 4); // Number of uncompr. bytes
    }

    /*
     * Writes integer in Intel byte order to a byte array, starting at a
     * given offset.
     */
    private void writeInt(int i, byte[] buf, int offset) throws IOException {
        writeShort(i & 0xffff, buf, offset);
        writeShort((i >> 16) & 0xffff, buf, offset + 2);
    }

    /*
     * Writes short integer in Intel byte order to a byte array, starting
     * at a given offset
     */
    private void writeShort(int s, byte[] buf, int offset) throws IOException {
        buf[offset] = (byte) (s & 0xff);
        buf[offset + 1] = (byte) ((s >> 8) & 0xff);
    }

    private int handleUserOptions(String[] args) {
        if (args.length == 0) {
            return -1;
        }
        if (args.length == 1) {
            System.err.println(
                    "Only one argument was used, where the only valid arguments are `-p NUM` which requires two args. Using as many threads as avalabile processors...");
        }
        // else we have at least 2 arguments

        boolean lastWasP = false;
        int p = -1;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-p")) {
                if (!lastWasP) {
                    lastWasP = true;
                    continue;
                } else {
                    System.err.println("Error in call sequence! detected '-p -p' ");
                    System.exit(1);
                }
            } else if (lastWasP) {
                try {
                    p = Integer.parseInt(args[i]);
                    if (p < 1) {
                        System.err.println(
                                "Error in call sequence! Recived a nonpositive number as the number of cores to use! Exiting...");
                        System.exit(1);
                    } else if (p > 4 * NUM_AVALBILE_CORES) {
                        System.err.println("You are trying to use " + Integer.toString(p)
                                + " while there are only " +
                                Integer.toString(NUM_AVALBILE_CORES)
                                + " processors avalabile. We will default to using 4 times the number of processors avalabile");
                        p = NUM_AVALBILE_CORES * 4;
                    }
                    break;
                } catch (NumberFormatException e) {
                    System.err.println(
                            "Error in call sequence! detected '-p' followed by something that is not a number");
                    System.err.println(args[i]);
                    System.exit(1);
                }
            } else {
                System.err.println("The only acceptable args are -p NUM, while we detected the argument " + args[i]
                        + ". Exiting...");
                System.exit(1);
            }
        }
        return p;
    }
}
