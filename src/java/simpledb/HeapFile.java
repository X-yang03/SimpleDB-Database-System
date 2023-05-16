package simpledb;

import java.io.*;
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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File File;
    private TupleDesc td;
    private BufferPool bp;

    private int numPage;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.File=f;
        this.td=td;
        bp=Database.getBufferPool();
        numPage = (int)(File.length()/(bp.getPageSize()));
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {

        return File;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return File.getAbsoluteFile().hashCode();
       // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int offset=bp.getPageSize()*pid.getPageNumber();
        Page page=null;
        RandomAccessFile random=null;
        byte[] data=new byte[bp.getPageSize()];
        try{
            random=new RandomAccessFile(File,"r");
            random.seek(offset);
            random.read(data,0,data.length);
            page=new HeapPage((HeapPageId) pid,data);
        }catch(IOException e){
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try(RandomAccessFile f = new RandomAccessFile(File,"rw")){
            f.seek(page.getId().getPageNumber()*bp.getPageSize());
            byte[] data = page.getPageData();
            f.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        numPage = (int)(File.length()/(bp.getPageSize()));
        return numPage;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> PageList = new ArrayList<Page>();
        for(int i=0;i<numPages();i++){
            HeapPage page = (HeapPage) bp.getPage(tid, new HeapPageId(this.getId(),i),Permissions.READ_WRITE);  //读取对应页
            if(page.getNumEmptySlots()==0) {     //full
                //added in lab4,when there's no empty slots,we could unlock the page
                bp.releasePage(tid,new HeapPageId(this.getId(),i));
                //this will do no harm to 2PL lock,cuz we didn't read any data
                continue;
            }
            page.insertTuple(t);
            PageList.add(page);
            //page.markDirty(true,tid); // added in lab4
            return PageList;
        }
        if(PageList.size()==0){    // all full
            HeapPageId newid = new HeapPageId(this.getId(),numPages());//ceate a new page,
            HeapPage blankPage = new HeapPage(newid,HeapPage.createEmptyPageData());
            numPage++;
            writePage(blankPage);
            HeapPage newPage = (HeapPage) bp.getPage(tid,newid,Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true,tid);
            PageList.add(newPage);
            return PageList;
        }
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = null;
        for(int i=0;i<numPages();i++){
            if(i == pid.getPageNumber()){
                page=(HeapPage) bp.getPage(tid,pid,Permissions.READ_WRITE);
                page.deleteTuple(t);
            }
        }
        if(page == null){
            throw new DbException("not in this table");
        }
        ArrayList<Page> lst = new ArrayList<>();
        lst.add(page);
        //page.markDirty(true,tid);//added in lab4
        return lst;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);

    }

    private class HeapFileIterator implements DbFileIterator {

        private TransactionId tid;
        private Iterator<Tuple> TupleIterator;
        private int nowPage;    //record the number of the page
        public HeapFileIterator(TransactionId tid) {
            this.tid=tid;
        }

        public Iterator<Tuple> getTuples(HeapPageId pid) throws TransactionAbortedException, DbException {
            HeapPage page=(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);  //find the page via pid
            return page.iterator();       //return the tuples in the page with id pid
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {  //initialize the iterator
            nowPage=0;  //begin with the first page
            HeapPageId pid=new HeapPageId(getId(),nowPage);
            TupleIterator=getTuples(pid);
        }
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(TupleIterator==null)
                return false;
            if(TupleIterator.hasNext()){
                return true;
            }
            while (TupleIterator != null && !TupleIterator.hasNext()) {
                if (nowPage < numPages()-1)  //When TupleIterator do not has a next() and it is not a null iterator
                {
                    nowPage++;
                    HeapPageId pid = new HeapPageId(getId(), nowPage);
                    TupleIterator = getTuples(pid);
                    //return TupleIterator.hasNext();
                } else{
                    TupleIterator = null;
                }
                    //return false;
            }
            if(TupleIterator == null)
                return false;
            return TupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()){
                throw new NoSuchElementException();
            }
            return TupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            TupleIterator=null;
        }
    }
}

