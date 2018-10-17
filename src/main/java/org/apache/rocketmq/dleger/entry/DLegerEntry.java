package org.apache.rocketmq.dleger.entry;

public class DLegerEntry {

    public final static int HEADER_SIZE = 4 + 4 + 8 + 8 + 4 + 4;

    private int magic;
    private int size;
    private long index;
    private long term;
    private int chainCrc; //like the block chain, this crc indicates any modification before this entry.
    private int bodyCrc; //the crc of the body
    private byte[] body;

    private transient long pos; //used to validate data

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getMagic() {
        return magic;
    }

    public void setMagic(int magic) {
        this.magic = magic;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public int getChainCrc() {
        return chainCrc;
    }

    public void setChainCrc(int chainCrc) {
        this.chainCrc = chainCrc;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getBodyCrc() {
        return bodyCrc;
    }

    public void setBodyCrc(int bodyCrc) {
        this.bodyCrc = bodyCrc;
    }

    public int computSizeInBytes() {
        size = HEADER_SIZE +  4 + body.length;
        return size;
    }
    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }
}