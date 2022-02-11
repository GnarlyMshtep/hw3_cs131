public class CompressedBlock {
    private byte[] block;
    private int blockLength;

    CompressedBlock(byte[] block, int blockLength) {
        this.block = block;
        this.blockLength = blockLength;
    }

    public byte[] getBlock() {
        return this.block;
    }

    public int getBlockLength() {
        return this.blockLength;
    }

}
