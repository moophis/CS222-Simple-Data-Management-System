#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <utility>
#include <functional>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cassert>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

// error enumerations
enum {
    ERR_INV_INIT_BUCKET     = -301,     // error: invalid initial buckets
    ERR_BAD_PAGE            = -302,     // error: bad page data
    ERR_OUT_OF_BOUND        = -303,     // error: index out of bound
    ERR_METADATA_MISSING    = -304,     // error: cannot find metadata
    ERR_METADATA_ERROR      = -305,     // error: metadata error
    ERR_NO_SPACE            = -306,     // error: page doesn't have enough space
    ERR_ENTRY_NOT_FOUND     = -307,     // error: cannot find the entry
    ERR_DUPLICATE_ENTRY     = -308,     // error: duplicate entry found in the same page
    ERR_INV_OPERATION       = -309,     // error: invalid operation
};

class IX_ScanIterator;
class IXFileHandle;
class ActivityManager;
class KeyValue;
class MetadataPage;
class DataPage;

class IndexManager {
  friend class IX_ScanIterator;
 public:
  static IndexManager* instance();

  // Create index file(s) to manage an index
  RC createFile(const string &fileName, const unsigned &numberOfPages);

  // Delete index file(s)
  RC destroyFile(const string &fileName);

  // Open an index and returns an IXFileHandle
  RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

  // Close an IXFileHandle.
  RC closeFile(IXFileHandle &ixfileHandle);


  // The following functions  are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For INT and REAL: use 4 bytes to store the value;
  //     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

  // Insert an entry to the given index that is indicated by the given IXFileHandle
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given IXFileHandle
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity

  // Initialize and IX_ScanIterator to supports a range search
  RC scan(IXFileHandle &ixfileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  // Generate and return the hash value (unsigned) for the given key
  unsigned hash(const Attribute &attribute, const void *key);


  // Print all index entries in a primary page including associated overflow pages
  // Format should be:
  // Number of total entries in the page (+ overflow pages) : ??
  // primary Page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx] [xx]
  // overflow Page No.?? liked to [primary | overflow] page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx]
  // where [xx] shows each entry.
  RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber);

  // Get the number of primary pages
  RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages);

  // Get the number of all pages (primary + overflow)
  RC getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages);

 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;
  PagedFileManager *_pfm;
  ActivityManager *_am;

 private:
  // Load all pages within a given bucket
  void loadBucketChain(vector<DataPage *> &buf, IXFileHandle &ixfileHandle,
                   unsigned bucketNum, const AttrType &keyType);

  // flush all pages within a given bucket
  RC flushBucketChain(vector<DataPage *> &buf);

  // Redistribute entries between two bucket chains (used for split operation)
  RC rebalanceBetween(IXFileHandle &ixfileHandle, unsigned oldBucket, vector<DataPage *> &oldCache,
          unsigned newBucket, vector<DataPage *> &newCache, MetadataPage &metadata, const Attribute &attribute);

  // Redistributed entries within a bucket chain (used when there is at least one emtpy page)
  // It can also check whether the bucket chain is empty.
  RC rebalanceWithin(IXFileHandle &ixfileHandle, unsigned bucket,
          vector<DataPage *> &cache, bool &emptyBucket, MetadataPage &metadata);

  // Insert an entry into a bucket given the buffer of the bucket
  RC insertInternal(vector<DataPage *> &cachedPages, KeyValue &keyValue,
          const RID &rid, bool &inserted);

  // Insert an entry in a new appended page at the end of the give bucket
  RC appendInternal(vector<DataPage *> &cachedPages, KeyValue &keyValue,
          const RID &rid, MetadataPage &metadata, IXFileHandle &ixfileHandle,
          const Attribute &attribute);

  // Insert an entry into bucket. Append a new page if necessary
  RC insertIntoBucket(vector<DataPage *> &cachedPages, KeyValue &keyValue,
          const RID &rid, MetadataPage &metadata, IXFileHandle &ixfileHandle,
          const Attribute &attribute);

  // Check whether the given bucket is empty
  bool isEmptyBucket(vector<DataPage *> &cache);

  // Grow primary page(s) until the file can hold up to the page of #pageNum
  RC growToFit(IXFileHandle &ixfileHandle, unsigned pageNum, const AttrType &keyType);

  // Find the bucket number according to the key and current state
  unsigned calcBucketNumber(KeyValue &keyValue, const Attribute &attribute, MetadataPage &metadata);

  // Print entries of each page
  RC printEntries(DataPage *page);
};

// Define (immutable) key value type (Int, Real, Varchar)
class KeyValue {
public:
    KeyValue() : _int(0), _float(0.0), _varchar(""), _size(0) {}
    KeyValue(int val) : _int(val), _keyType(TypeInt), _size(sizeof(int)) {}
    KeyValue(float val) : _float(val), _keyType(TypeReal), _size(sizeof(float)) {}
    KeyValue(string val) : _varchar(val), _keyType(TypeVarChar), _size(sizeof(int) + val.size()) {}
    // Building key value from raw data
    KeyValue(const void *data, AttrType keyType) : _keyType(keyType) {
        if (!data) {
            KeyValue();
        } else {
            switch (_keyType) {
            case TypeInt:
                memcpy((char *) &_int, (char *) data, sizeof(int));
                _size = sizeof(int);
                break;
            case TypeReal:
                memcpy((char *) &_float, (char *) data, sizeof(float));
                _size = sizeof(float);
                break;
            case TypeVarChar:
                int size;
                memcpy((char *) &size, (char *) data, sizeof(int));
                assert(size < PAGE_SIZE && size > 0);
                char buf[PAGE_SIZE];
                memcpy((char *) buf, (char *) data + sizeof(int), size);
                buf[size] = 0;
                _varchar.assign(buf);
                _size = sizeof(int) + size;
                break;
            default:
                __trace();
                break;
            }
        }
    }

    // Get the raw data (void *)
    void getRaw(void *data) {
        switch (_keyType) {
        case TypeInt:
            memcpy((char *) data, (char *) &_int, sizeof(int));
            break;
        case TypeReal:
            memcpy((char *) data, (char *) &_float, sizeof(float));
            break;
        case TypeVarChar: {
            size_t size = _varchar.size();
            memcpy((char *) data, (char *) &size, sizeof(int));
            memcpy((char *) data + sizeof(int), (char *) _varchar.c_str(), size);
            break;
        }
        default:
            __trace();
            break;
        }
    }

    // Get value (Note that only value fetch operation with match
    // type will succeed)
    bool getInt(int &val) {
        if (_keyType == TypeInt) {
            val = _int;
            return true;
        }
        return false;
    }

    bool getReal(float &val) {
        if (_keyType == TypeReal) {
            val = _float;
            return true;
        }
        return false;
    }

    bool getVarChar(string &val) {
        if (_keyType == TypeVarChar) {
            val = _varchar;
            return true;
        }
        return false;
    }

    // Get type
    AttrType getType() { return _keyType; }

    // Get key size
    size_t size() { return _size; }

    // Print data
    void printData() {
        switch (_keyType) {
        case TypeInt:
            std::cout << "Value TypeInt: " << _int << std::endl;
            break;
        case TypeReal:
            std::cout << "Value TypeReal: " << _float << std::endl;
            break;
        case TypeVarChar:
            std::cout << "Value TypeVarChar: " << _varchar << std::endl;
            break;
        default:
            std::cout << "Unknown type!" << std::endl;
            break;
        }
    }

    // Format data as a string
    string toString() {
        stringstream ss;
        switch (_keyType) {
        case TypeInt:
            ss << _int;
            return ss.str();
        case TypeReal:
            ss << _float;
            return ss.str();
        case TypeVarChar:
            return _varchar;
        default:
            __trace();
            return "";
        }
    }

    // Hash code of the key value
    unsigned hashCode() {
        switch (_keyType) {
        case TypeInt:
            std::hash<int> intHash;
            return intHash(_int);
        case TypeReal:
            std::hash<float> realHash;
            return realHash(_float);
        case TypeVarChar:
            std::hash<string> varCharHash;
            return varCharHash(_varchar);
        default:
            __trace();
            return 0;
        }
    }

    // Comparison function
    // return 0 if equal, 1 if this > that, -1 if this < that
    int compare(KeyValue &that) {
        assert(_keyType == that.getType());
        switch (_keyType) {
        case TypeInt:
            int ival;
            that.getInt(ival);
            return (_int == ival) ? 0 : ((_int > ival) ? 1 : -1);
        case TypeReal:
            float rval;
            that.getReal(rval);
            return (_float == rval) ? 0 : ((_float > rval) ? 1 : -1);
        case TypeVarChar: {
            string sval;
            that.getVarChar(sval);
            return _varchar.compare(sval);
        }
        default:
            __trace();
            return PAGE_SIZE;
        }
    }

private:
    int _int;
    float _float;
    string _varchar;
    AttrType _keyType;
    size_t _size;
};

// Index File Handle
class IXFileHandle {
    friend class IndexManager;
    friend class IX_ScanIterator;

public:
    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    IXFileHandle();                             // Constructor
    ~IXFileHandle();                            // Destructor

private:
    FileHandle _primaryHandle;
    FileHandle _overflowHandle;
};

// Scan Iterator
typedef enum {
    HASH_SCAN,
    RANGE_SCAN,
} ScanType;

class IX_ScanIterator {
  friend class IndexManager;
public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan

private:
  // Get next exact match entry
  RC getNextHashMatch(RID &rid, void *key);
  // Get next range match entry
  RC getNextRangeMatch(RID &rid, void *key);

private:
  IndexManager *_ixm;           // Instance of the index manager
  bool _active;                 // Indicate whether the iterator is active
  ScanType _scanType;           // Type of scan (hash or range)
  IXFileHandle _ixFileHandle;   // File handle
  bool _hasLowerBound;
  bool _hasUpperBound;
  bool _lowInclusive;
  bool _highInclusive;
  KeyValue _lowKey;
  KeyValue _highKey;
  AttrType _keyType;

  // Cache the entire bucket chain
  vector<DataPage *> _curBucket;
  unsigned _curBucketNum;   // the # of currently buffered bucket
  unsigned _totalBucketNum; // total bucket number
  unsigned _curPageIndex;   // _curBucket[_curPageIndex]
  unsigned _curHashIndex;   // Used in hash scan: _entryMap[key][_curHashIndex]
  unsigned _curRangeIndex;  // Used in range scan: _keys[_curRangeIndex]
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


// The class to manipulate metadata page
class MetadataPage {
    friend class IndexManager;
private:
    // metadata page resides in the 1st page of overflow page file
    static const int METADATA_PAGENUM = 0;

    // The following variables are sequentially aligned in overflow page file
    unsigned _entryCount;       // total count of entries in the index
    unsigned _primaryPageCount; // total count of primary pages (current level pages + new split pages in this level)
    unsigned _overflowPageCount; // total count of overflow pages (including lazily deleted ones)
    unsigned _delOverflowPageCount; // # of deleted overflow page count
    unsigned _currentBucketCount;  // the # of current buckets (primary pages)
    unsigned _nextSplitBucket;     // the next page to be split
    unsigned _initialBucketCount;  // the initial # of bucket (power of 2)

    FileHandle &_fileHandle;        // associated file handle to the metadata file
    bool _initialized;             // whether the page has been initialized
    bool _dirty;                   // indicate whether the page has been changed

public:
    MetadataPage(FileHandle &handle);
    ~MetadataPage();

    // initialize the metadata page
    RC initialize(const unsigned &numberOfPages);

    // load the metadata page
    RC load();

    // write back the metadata into the file
    RC flush();

    void printMetadata();

    unsigned getEntryCount();
    void setEntryCount(unsigned entryCount);
    unsigned getPrimaryPageCount();
    void setPrimaryPageCount(unsigned primaryPageCount);
    unsigned getOverflowPageCount();
    void setOverflowPageCount(unsigned overflowPageCount);
    unsigned getDelOverflowPageCount();
    void setDelOverflowPageCount(unsigned delOverflowPageCount);
    unsigned getCurrentBucketCount();
    void setCurrentBucketCount(unsigned currentBucketCount);
    unsigned getNextSplitBucket();
    void setNextSplitBucket(unsigned nextSplitBucket);
    unsigned getInitialBucketCount();
    bool isInitialized();
    void setInitialized(bool initialized);
};


typedef enum {
    PRIMARY_PAGE = 0,
    OVERFLOW_PAGE,
} PageType;

#define META_UNIT   sizeof(int) // unit size of a metadata slot
#define PAGE_END    0           // indicating the end of a page chain (overflow page num start from 1)

// The class to manipulate data page (primary or overflow)
class DataPage {
    friend class IndexManager;
private:
    FileHandle &_fileHandle;    // associated file handle to the metadata file

    PageType _pageType;        // (primary or overflow) @ PAGE_SIZE - META_UNIT
    AttrType _keyType;         // (Int, Real, Varchar) @ PAGE_SIZE - META_UNIT * 2
    unsigned _pageNum;         // the page # in the (primary or overflow) file @ PAGE_SIZE - META_UNIT * 3
    unsigned _entriesCount;    // # of entries in the page @ PAGE_SIZE - META_UNIT * 4
    unsigned _entriesSize;     // total size of all entries in the page @ PAGE_SIZE - META_UNIT * 5
    unsigned _nextPageNum;     // the next page the current one points to (PAGE_END if no more) @ PAGE_SIZE - META_UNIT * 6

    vector<KeyValue> _keys;                         // Buffered data: keys
    vector<RID> _rids;                              // Buffered data: RIDs

    unordered_map<string, vector<int> > _entryMap;   // Buffered data: map: key value (as string> -> vector of positions

    bool _dirty;           // indicate whether the page has been changed
    bool _discarded;       // indicate whether the page is not used anymore

public:
    // Constructor: get an exiting page (newPage = true -> create a new page rather than load existing one)
    DataPage(FileHandle &fileHandle, PageType pageType,
            AttrType keyType, unsigned pageNum, bool newPage);
    // Destructor will flush the in-memory page back to file
    ~DataPage();

    RC initialize();
    RC load();
    RC flush();

    // Discard the current page.
    // That means the in-memory data will not be flushed.
    void discard();

    // Find the key value given index
    RC keyAt(unsigned index, KeyValue &key);

    // Find RID given index
    RC ridAt(unsigned index, RID &rid);

    // Find indexes of entries with specified key
    RC findKeyIndexes(KeyValue &key, vector<int> &indexes);

    // Check whether there are still space to store a new entry
    bool hasSpace(KeyValue &key);

    // Check whether a <key, RID> pair exists in the page
    bool doExist(KeyValue &key, const RID &rid);

    // Insert a <key, RID> pair in the current page
    RC insert(KeyValue &key, const RID &rid);

    // Remove a <key, RID> par in the current page
    RC remove(KeyValue &key, const RID &rid);

    // Print metadata of the page
    void printMetadata();

    PageType getPageType();
    void setPageType(PageType pageType);
    AttrType getKeyType();
    void setKeyType(AttrType keyType);
    unsigned getPageNum();
    void setPageNum(unsigned pageNum);
    unsigned getEntriesCount();
    void setEntriesCount(unsigned entriesCount);
    unsigned getEntriesSize();
    void setEntriesSize(unsigned entriesSize);
    unsigned getNextPageNum();
    void setNextPageNum(unsigned nextPageNum);

private:
    void wireMetadata(void *page);
    void loadMetadata(void *page);
    void serializeData(void *page);
    void deserializeData(void *page);
    size_t entrySize(KeyValue &key);
};

typedef enum {
    INITIAL = 0,
    SCAN,
    INSERT,
    DELETE,
} IndexOperation;

// Activity Manager: record the most recent activity
// This class is specially used for scan iterator to help it indicate whether the page
// it is querying on is dirty in order to save I/O.
class ActivityManager {
private:
    IndexOperation _lastOp;  // last page related operation
    PageType _lastPageType;  // type of last accessed page
    unsigned _lastPageNum;   // # of last accessed page
    static ActivityManager *_instance;

protected:
    ActivityManager() : _lastOp(INITIAL), _lastPageType(PRIMARY_PAGE), _lastPageNum(0) {}

public:
    static ActivityManager *instance() {
        if (!_instance) {
            _instance = new ActivityManager();
        }
        return _instance;
    }

    void setLast(IndexOperation op, PageType type, unsigned pageNum) {
        _lastOp = op;
        _lastPageType = type;
        _lastPageNum = pageNum;
    }

    IndexOperation getIndexOperation() { return _lastOp; }
    PageType getPageType() { return _lastPageType; }
    unsigned getLastPageNum() { return _lastPageNum; }
};

#endif
