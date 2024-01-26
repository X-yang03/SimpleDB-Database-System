package simpledb;

import javafx.print.PageLayout;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    public int numPages;
    public ConcurrentHashMap<PageId,Page> idToPage;

    private  LinkedList<Page> recentUsedPages;

    private class Lock{
        public static final int SHARE = 0;  //0 stands for shared lock,
        // Multiple transactions can have a shared lock on an object before reading
        public static final int EXCLUSIVE = 1;  //1 stands for exclusive lock,
        // Only one transaction may have an exclusive lock on an object before writing
        private TransactionId tid;
        private int type;

        public Lock(TransactionId tid, int type){
            this.tid =  tid;
            this.type = type;
        }
        public TransactionId getTid() {
            return tid;
        }
        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }

    private class LockManager{
        private ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,Lock>> pageLocks;
        //use concurrentHashMap instead of HashMap cuz it's safer than the latter
        //when facing multiple threads

        public LockManager(){
            pageLocks = new ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,Lock>>();
        }

        public synchronized boolean acqureLock(PageId pid,TransactionId tid,int lockType)
                throws TransactionAbortedException {
            if(pageLocks.get(pid)==null){  // no exsisting locks, create newPageLock and return ture
                Lock newLock = new Lock(tid,lockType);
                ConcurrentHashMap<TransactionId,Lock> newPageLock = new ConcurrentHashMap<>();
                newPageLock.put(tid,newLock);
                pageLocks.put(pid,newPageLock);
                return true;
            }
            //if there's locks already
            ConcurrentHashMap<TransactionId,Lock> nowPageLock = pageLocks.get(pid);

            if(nowPageLock.get(tid)==null){   //no locks from tid
                if(nowPageLock.size()>1){  //exists locks from other transactions
                    if(lockType == Lock.SHARE){
                        Lock newLock = new Lock(tid,lockType); //if requring a read lock
                        nowPageLock.put(tid,newLock);
                        pageLocks.put(pid,nowPageLock);
                        return true;
                    }
                    else{  //requiring a write lock
                        return false;
                    }
                }
                else if(nowPageLock.size()==1){  //only one other lock,could be a read or write lock
                    Lock existLock = null;
                    for(Lock lock:nowPageLock.values())
                        existLock = lock;
                    if(existLock.getType() == Lock.SHARE){  // if it is a read lock
                        if(lockType == Lock.SHARE){// require a read lock too, return true
                            Lock newLock = new Lock(tid,lockType);
                            nowPageLock.put(tid,newLock);
                            pageLocks.put(pid,nowPageLock);
                            return true;
                        }
                        else if(lockType == Lock.EXCLUSIVE){
                            return false;
                        }
                    }
                    else if(existLock.getType() == Lock.EXCLUSIVE){ //some transaction is writing this page
                        return false;
                    }
                }
            }
            else{
                Lock preLock = nowPageLock.get(tid);
                if(preLock.getType() == Lock.SHARE){  //if previous lock is a read lock
                    if(lockType == Lock.SHARE){ //and require a read lock too
                        return true;
                    }
                    else{ // if want a write lock
                        if(nowPageLock.size() == 1){ // if only this tid hold a lock,we could update it
                            preLock.setType(Lock.EXCLUSIVE);
                            nowPageLock.put(tid,preLock);
                            return true;
                        }
                        else{
                            throw new TransactionAbortedException();
                        }
                    }
                }

                else if(preLock.getType() == Lock.EXCLUSIVE){ //the previous one is a write lock
                    //means no other locks on this page
                    return true;
                }
            }
            return false;
        }

        public synchronized boolean holdsLock(TransactionId tid,PageId pid){
            if(pageLocks.get(pid) == null){
                return false;
            }
            ConcurrentHashMap<TransactionId,Lock> nowPageLock = pageLocks.get(pid);
            if(nowPageLock.get(tid) == null){
                return false;
            }
            return true;
        }

        public  synchronized boolean releaseLock(TransactionId tid,PageId pid){
            if(holdsLock(tid,pid)){
                ConcurrentHashMap<TransactionId,Lock> nowPageLock = pageLocks.get(pid);
                nowPageLock.remove(tid);
                if(nowPageLock.size() == 0){
                    pageLocks.remove(pid);
                }
                this.notifyAll();
                return true;

            }
            return false;
        }

        public synchronized boolean TransactonCommitted(TransactionId tid){ // when a transaction completes,release all the locks
            for(PageId pid : pageLocks.keySet()){
                releaseLock(tid,pid);
            }
            return true;
        }

    }

    private LockManager manager;

    public BufferPool(int numPages) {
        // some code goes here
        this.numPages=numPages;
        idToPage=new ConcurrentHashMap<>(numPages);
        recentUsedPages = new LinkedList<Page>();  //used to evict page
        manager = new LockManager(); //create a lock manager
    }


    private synchronized void moveToHead(PageId pid){

        String thread = Thread.currentThread().getName();
        //System.out.println("modify from "+thread);
        for(int i = 0;i<recentUsedPages.size();i++){
            Page page = recentUsedPages.get(i);
            if(page.getId().equals(pid)) {
                recentUsedPages.remove(i);
                recentUsedPages.add(0,page);    //find the required page,and put it at the top as the recent used page
                break;
            }
        }
        //System.out.println("done from "+thread);
    }

    private synchronized void addRecentUsed(Page page){
        recentUsedPages.add(0,page);
    }

    private synchronized void delRecentUsed(PageId pid){

        for(int i = 0;i<recentUsedPages.size();i++){
            Page page = recentUsedPages.get(i);
            if(pid.equals(page.getId())) {
                recentUsedPages.remove(i);
                break;
            }

        }
    }
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int lockType = (perm == Permissions.READ_ONLY)? Lock.SHARE : Lock.EXCLUSIVE;
        //only READ_ONLY stands for a shared lock
        long begin = System.currentTimeMillis();
        boolean ifAcquired = false;
        while (!ifAcquired){
            ifAcquired = manager.acqureLock(pid,tid,lockType);
            long rightNow = System.currentTimeMillis();
            if(rightNow - begin > 500) {  //timeout policy
                //System.out.println("time out"+begin+"  "+rightNow);
                throw new TransactionAbortedException();
            }
        }

//----------------before lab4 -----------------------------------------
        if(idToPage.containsKey(pid)){    //if Page pid does exist, return the page
            int index = 0;          //if the Page exists, then the list recentUsedPages must contain it too
            //moveToHead(pid);
            return idToPage.get(pid);
        }
        else {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page newPage =  file.readPage(pid);  //use the abstract class Page
            if(idToPage.size() >= numPages){
                // Using LRU algorithm to evict the last used Page
                evictPage();
            }
            idToPage.put(pid,newPage);  //When there's no valid page in BufferPool, find it in the disk and put it into BufferPool
            //recentUsedPages.add(0,newPage);  //add the new Page to the top of the list
            addRecentUsed(newPage);
            String t = Thread.currentThread().getName();
            //System.out.println("add from"+t);
            return newPage;
        }
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        manager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        manager.TransactonCommitted(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return manager.holdsLock(tid,p);
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
        if(commit){
            try {
                flushPages(tid);
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        else{ //recovery
            for(Page page : idToPage.values()){
                PageId pid = page.getId();
                if(tid.equals(page.isDirty())){    //if tid has modified this page,we try to recover it
                    int tableId = pid.getTableId();
                    DbFile file = Database.getCatalog().getDatabaseFile(tableId);
                    Page recoverPage = file.readPage(pid);   //reload the page
                    idToPage.put(pid,recoverPage);
                    recentUsedPages.add(0,recoverPage);  //reput it
                }
            }
        }
        manager.TransactonCommitted(tid);
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
        // some code goes here
        DbFile table =  Database.getCatalog().getDatabaseFile(tableId);  //modified in lab5, use DbFile instead of HeapFile
        ArrayList<Page> affectedPages = table.insertTuple(tid,t);
        for(Page page : affectedPages){
            page.markDirty(true, tid);
            if (idToPage.size() == numPages) {
                evictPage();          //when bufferpool is full, evict page
            }
            if(idToPage.containsKey(page.getId())){  //if map contains the page already ,we connot simply add page to the list
                delRecentUsed(page.getId());
            }
            idToPage.put(page.getId(), page);

            recentUsedPages.add(0,page);
        }
        // not necessary for lab1
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
        // some code goes here
        int tableid = t.getRecordId().getPageId().getTableId();
        DbFile table = Database.getCatalog().getDatabaseFile(tableid);
        ArrayList<Page> affectedPages = table.deleteTuple(tid,t);
        for(Page page : affectedPages){
            page.markDirty(true, tid);
            if (idToPage.size() == numPages) {
                evictPage();
            }
            if(idToPage.containsKey(page.getId())){  //if map contains the page already ,we connot simply add page to the list
               delRecentUsed(page.getId());
            }
            idToPage.put(page.getId(), page);
            recentUsedPages.add(0,page);
        }
        // not necessary for lab1
    }


    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for(Map.Entry<PageId , Page> entry : idToPage.entrySet()){
            Page page = entry.getValue();
            if(page.isDirty()!=null){
                flushPage(page.getId());
            }
        }
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        idToPage.remove(pid);   // delete this page
        delRecentUsed(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        Page dirty_page =idToPage.get(pid);
        DbFile table =  Database.getCatalog().getDatabaseFile(pid.getTableId());
        table.writePage(dirty_page);  //write any dirty page to disk and mark it as not dirty, while leaving it in the BufferPool
        dirty_page.markDirty(false, null);
        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(Page page : idToPage.values()){
            PageId pid = page.getId();
            if(tid.equals(page.isDirty())){
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int length = recentUsedPages.size()-1;
        while (recentUsedPages.get(length).isDirty()!=null){  //NO STEAL policy
            length--;  //if it is a dirty page,do not evict it
            if(length<0)
                throw new DbException("all pages are dirty");
        }
        Page page = recentUsedPages.remove(length);
        /*Page page = recentUsedPages.removeLast();  //find the last used page
        try{
            flushPage(page.getId());    //flush this page to the disk
        }catch (IOException e){
            e.printStackTrace();
        }*/
        discardPage(page.getId());   //remove it from the BufferPool

    }

}
