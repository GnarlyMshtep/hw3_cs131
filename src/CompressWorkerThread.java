import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.Deflater;

public class CompressWorkerThread implements Runnable {
    private int serialNumCount;
    private ConcurrentHashMap<Integer, CompressedBlock> blockStorage;
    private byte[] blockBuf;
    int numBytesToCompress;

    byte[] dictBuf;
    boolean hasDict;

    Deflater compressor;

    boolean isLastBlock;

    CompressWorkerThread(int serialNumCount, ConcurrentHashMap<Integer, CompressedBlock> blockStorage, byte[] blockBuf,
            int numBytesToCompress, byte[] dictBuf, boolean hasDict, boolean isLastBlock) {
        this.serialNumCount = serialNumCount;
        this.blockStorage = blockStorage;
        this.blockBuf = blockBuf;
        this.numBytesToCompress = numBytesToCompress;

        this.dictBuf = dictBuf;
        this.hasDict = hasDict;

        compressor = new Deflater(Deflater.DEFAULT_COMPRESSION, true);

        this.isLastBlock = isLastBlock;
    }

    public void run() {
        byte[] tempBlockBuff = new byte[numBytesToCompress * 2];
        int totalBytesCompressed = 0;

        if (hasDict) {
            compressor.setDictionary(dictBuf);
        }

        compressor.setInput(blockBuf, 0, numBytesToCompress);

        if (isLastBlock) {

            if (!compressor.finished()) {
                compressor.finish();
                while (!compressor.finished()) {
                    int deflatedBytes = compressor.deflate(
                            tempBlockBuff, 0, tempBlockBuff.length, Deflater.NO_FLUSH);
                    if (deflatedBytes > 0) {
                        blockStorage.put(serialNumCount, new CompressedBlock(tempBlockBuff, deflatedBytes));
                    }
                }
            }
        } else {

            int deflatedBytes = compressor.deflate(
                    tempBlockBuff, 0, tempBlockBuff.length, Deflater.SYNC_FLUSH);
            blockStorage.put(serialNumCount, new CompressedBlock(tempBlockBuff, deflatedBytes));
        }

    }
}
