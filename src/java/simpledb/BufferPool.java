package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    // MARK: New Implemented Eviction algorithm!
    private class EVNode implements Comparable<EVNode> {
        private int count;
        private Date time;
        private final PageId key;
        private final Page value;

        EVNode(int count, Date time, PageId key, Page value) {
            this.count = count;
            this.time = time;
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            else if (o instanceof EVNode) {
                EVNode lObj = (EVNode)o;
                return this.count == lObj.count && this.time.equals(lObj.time);
            } else return false;
        }

        @Override
        public int compareTo(EVNode o) {
            if (this.count != o.count) {
                return o.count - this.count;
            } else {
                return this.time.compareTo(o.time);
            }
        }

        public int getCount() {
            return this.count;
        }

        public void makeUse() {
            this.count++;
            this.time = new Date();
        }

        public PageId getPageId() {
            return this.key;
        }

        public Page getPage() {
            return this.value;
        }
    }

    /*
     evManPoolMap: used to store all the buffers. Man means Manage
     evManSet: A set used to automatically sort node using Tree.
     */
    private ConcurrentHashMap<PageId, EVNode> evManPoolMap;
    private ConcurrentSkipListSet<EVNode> evManSet;
    private final int pool_max_size;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.pool_max_size = numPages;
        this.evManPoolMap = new ConcurrentHashMap<>();
        this.evManSet = new ConcurrentSkipListSet<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    	System.out.println("THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!");
    }

    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
        System.out.println("THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!");
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        Page retPage = null;
        boolean to_dirty = (perm == Permissions.READ_WRITE);
        if (this.evManPoolMap.containsKey(pid)) {
            // we get the old key of PageId. So we must update it's use count!
            EVNode pageEvData = this.evManPoolMap.get(pid);
            this.evManSet.remove(pageEvData);
            // count + 1
            pageEvData.makeUse();
            this.evManSet.add(pageEvData);
            retPage = pageEvData.getPage();
        } else {
            // we didn't find, so we call Table's File to read the page.
            int tableId = pid.getTableId();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
            Page readPage = dbFile.readPage(pid);
            if (this.evManPoolMap.size() >= this.pool_max_size) {
                // exceed max size. So we must evict a page.
                this.evictPage();
            }
            EVNode pageEvData = new EVNode(1, new Date(), pid, readPage);
            this.evManPoolMap.put(pid, pageEvData);
            this.evManSet.add(pageEvData);
            retPage = readPage;
        }
        if (to_dirty) {
            retPage.markDirty(true, tid);
        }
        return retPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        dbFile.insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        dbFile.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // iterate over all the page
        ArrayList<Page> flushList = new ArrayList<>();
        for (EVNode evNode: this.evManPoolMap.values()) {
            if (evNode.getPage().isDirty() != null) {
                flushList.add(evNode.getPage());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        EVNode evNode = this.evManPoolMap.get(pid);
        if (evNode != null) {
            this.evManSet.remove(evNode);
            this.evManPoolMap.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // first find the page
        // then find the corresponding Table.
        // then: write the page to the correct position in Table's related File.
        // then: mark the page as non-dirty.
        Page page = this.evManPoolMap.get(pid).getPage();
        int tableId = pid.getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        EVNode evictData;
        Iterator<EVNode> it = this.evManSet.iterator();
        if (!it.hasNext()) {
            throw new DbException("BufferPool: Pool is empty.");
        }
        while (it.hasNext()) {
            evictData = it.next();
            Page evictPage = evictData.getPage();
            if (evictPage.isDirty() == null) {
                PageId evictPgId = evictData.getPageId();
                this.evManSet.remove(evictData);
                this.evManPoolMap.remove(evictPgId);
                return;
            }
        }
        throw new DbException("BufferPool: All pages is dirty, cannot evict.");
    }

}
