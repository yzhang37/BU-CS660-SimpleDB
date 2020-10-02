package simpledb;

import java.util.*;

import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private final HeapFile f;
    private Iterator<Tuple> it;
    private final TransactionId tid;
    private int pageNum;

    public HeapFileIterator(TransactionId tid, HeapFile f) {
        this.tid = tid;
        this.f = f;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.pageNum = 0;
        this.it = this.getTupleListFromPage(this.pageNum).iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (this.it == null) {
            return false;
        }
        if (this.it.hasNext()) {
            return true;
        } else if (this.pageNum < this.f.numPages() - 1) {
            return this.getTupleListFromPage(this.pageNum + 1).size() > 0;
        } else {
            return false;
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (this.it == null) {
            throw new NoSuchElementException();
        }
        if (this.it.hasNext()) {
            return this.it.next();
        } else if (this.pageNum < this.f.numPages() - 1) {
            // we can continue access the next page
            this.pageNum++;
            it = this.getTupleListFromPage(this.pageNum).iterator();
            if (it.hasNext())
                return it.next();
            else {
                throw new NoSuchElementException();
            }

        } else {
            // no more tuples on current page and no more pages in file
            throw new NoSuchElementException();
        }
    }

    private List<Tuple> getTupleListFromPage(int pageNum) throws TransactionAbortedException, DbException {
        PageId pageId = new HeapPageId(this.f.getId(), pageNum);
        Page page = Database.getBufferPool().getPage(this.tid, pageId, Permissions.READ_ONLY);

        var list = new ArrayList<Tuple>();
        HeapPage heapPage = (HeapPage)page;
        Iterator<Tuple> itr = heapPage.iterator();
        while(itr.hasNext()){
            list.add(itr.next());
        }
        return list;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    public void close() {
        it = null;
    }
}
