package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int offset = pid.pageNumber() * pageSize;
        try {
            RandomAccessFile ranFile = new RandomAccessFile(this.f, "r");
            ranFile.seek(offset);
            byte[] b = new byte[pageSize];
            ranFile.read(b);
            ranFile.close();
            return new HeapPage((HeapPageId)pid, b);
        }
        catch (Exception ex) {
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        int offset = page.getId().pageNumber() * pageSize;
        RandomAccessFile ranFile = new RandomAccessFile(this.f, "w");
        ranFile.seek(offset);
        ranFile.write(page.getPageData());
        ranFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int pageSize = BufferPool.getPageSize();
        long fileSize = this.f.length();
        return (int)(fileSize / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> ret_list = new ArrayList<>();
        // now we can fetch pages from BufferPool
        // ==============================================================================
        // * since we may modify the page, so we must get the page from BufferPool with
        //   perms sent.
        // * we first try to find exists non-full pages.
        for (int i = 0; i < this.numPages(); ++i) {
            Page page = Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), null);
            if (!(page instanceof HeapPage)) {
                throw new DbException("Not a HeapPage, but this shouldn't happen!");
            }
            HeapPage heapPage = (HeapPage)page;
            // here we directly call heapPage.insertTuple. Since if it's full,
            // a DbException will be thrown from itself.
            try {
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                ret_list.add(heapPage);
                return ret_list;
            } catch (DbException ex) {}
        }
        // * if we cannot find non-full existing pages. Then we have to create a new one.
        //   First we write the page to the disk, and the load it to the BufferPool.
        byte[] newPageData = new byte[BufferPool.getPageSize()];
        Arrays.fill(newPageData, (byte)0);
        HeapPageId newHpPgId = new HeapPageId(this.getId(), this.numPages());
        HeapPage newHpPage = new HeapPage(newHpPgId, newPageData);
        this.writePage(newHpPage);
        // now we try to use the getPage function from BufferPool to load the new page into it.
        HeapPage newPage = (HeapPage)Database.getBufferPool().getPage(tid, newHpPgId, null);
        // here if DbException still throws, there must be some error
        // that this function cannot handle.
        newPage.insertTuple(t);
        ret_list.add(newPage);
        return ret_list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> ret_list = new ArrayList<>();
        for (int i = 0; i < this.numPages(); ++i) {
            Page page = Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), null);
            if (!(page instanceof HeapPage)) {
                throw new DbException("Not a HeapPage, but this shouldn't happen!");
            }
            HeapPage heapPage = (HeapPage)page;
            try {
                heapPage.deleteTuple(t);
                heapPage.markDirty(true, tid);
                ret_list.add(heapPage);
                return ret_list;
            } catch (DbException ex) {}
        }
        throw new DbException("HeapFile: Failed to Delete Tuple, due to tuple not found.");
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

