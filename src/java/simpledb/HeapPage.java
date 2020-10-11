package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;
    private int usedSlots;

    // isDirty: mark whether this page is dirty or not;
    boolean dirty = false;
    TransactionId tid = null;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        this.usedSlots = this.preCalcUsedSlots();

        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        double page_size = BufferPool.getPageSize();
        double tuple_size = this.td.getSize();
        return (int)Math.floor((page_size * 8) / (tuple_size * 8 + 1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return (int)Math.ceil(((double)this.getNumTuples()) / 8);
    }

    // MARK: This function is added by me.
    /**
     * Pre-calc the used slots from the Header
     * @return the value calculated via the current header array.
     */
    private int preCalcUsedSlots() {
        int used_count = 0;
        int i = 0;
        for (; i < this.header.length - 1; ++i) {
            int byteNum = this.header[i] & 0xFF;
            while (byteNum != 0) {
                if ((byteNum & 1) != 0)
                    used_count++;
                byteNum >>= 1;
            }
        }
        // now we handle the final header byte, which might not be full.
        int lastByteNum = this.header[i] & 0xFF, final_left = this.numSlots % 8;
        while (final_left > 0 && lastByteNum != 0) {
            if ((lastByteNum & 1) != 0)
                used_count++;
            lastByteNum >>= 1;
            final_left--;
        }
        return used_count;
    }

    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // here we need to first find the Tuple's place
        for (int i = 0; i < this.numSlots; ++i) {
            if (this.isSlotUsed(i)) {
                if (t.equals(this.tuples[i])) {
                    this.tuples[i] = null;
                    this.markSlotUsed(i, false);
                    t.setRecordId(null);
                    return;
                }
            }
        }
        throw new DbException("Cannot find tuple.");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        if (!(t.getTupleDesc().equals(this.td))) {
            throw new DbException("TupleDesc doesn't match");
        }
        if (this.getNumEmptySlots() == 0) {
            throw new DbException("Page is full.");
        }
        for (int i = 0; i < this.numSlots; ++i) {
            // find the corresponding place,
            if (!this.isSlotUsed(i)) {
                // register RecordId inside the tuple.
                t.setRecordId(new RecordId(this.getId(), i));
                // "copy" the tuple
                this.tuples[i] = t;
                // mark used.
                this.markSlotUsed(i, true);
                break;
            }
        }
        throw new DbException("Page should be full.");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        if (dirty) {
            this.dirty = true;
            this.tid = tid;
        } else {
            this.dirty = false;
            this.tid = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return this.tid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // FIX:
        // I think this function is of low efficiency.
        // because in the last byte, some bit might not be used. So
        // use this count to solve that problem.
        return this.numSlots - this.usedSlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int a = i / 8, b = i % 8;
        int val = this.header[a] & 0xFF;
        val >>= b;
        return (val & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        int a = i / 8, b = i % 8;
        // get the original value.
        boolean old = this.isSlotUsed(i);
        int sub = 1 << b;
        if (value) {
            this.header[a] = (byte)(this.header[a] | sub);
            if (!old) {
                // Because the original is not used, then we must make usedSlots + 1
                this.usedSlots++;
            }
        } else {
            this.header[a] = (byte)(this.header[a] & ~sub);
            if (old) {
                this.usedSlots--;
            }
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        ArrayList<Tuple> tmpList = new ArrayList<>();
        for (int i = 0; i < this.numSlots; ++i) {
            if (isSlotUsed((i))) {
                tmpList.add(this.tuples[i]);
            }
        }
        return tmpList.iterator();
    }

}

