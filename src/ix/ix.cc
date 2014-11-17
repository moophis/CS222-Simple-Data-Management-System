
#include "ix.h"

#define PRIMARY_SUFFIX  ".pp"
#define OVERFLOW_SUFFIX ".op"

ActivityManager *ActivityManager::_instance = 0;

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    _pfm = PagedFileManager::instance();
    _am  = ActivityManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
    RC err;
    string primary = fileName + PRIMARY_SUFFIX;
    string overflow = fileName + OVERFLOW_SUFFIX;

    // Initial bucket should be power of 2
    if (numberOfPages & (numberOfPages - 1)) {
        return ERR_INV_INIT_BUCKET;
    }

    if ((err = _pfm->createFile(primary.c_str())) != SUCCESSFUL) {
        return err;
    }
    if ((err = _pfm->createFile(overflow.c_str())) != SUCCESSFUL) {
        return err;
    }

    FileHandle h2;
    if ((err = _pfm->openFile(overflow.c_str(), h2)) != SUCCESSFUL) {
        return err;
    }

    MetadataPage metadataPage(h2);
    metadataPage.initialize(numberOfPages);

    if ((err = _pfm->closeFile(h2)) != SUCCESSFUL) {
        return err;
    }

	return SUCCESSFUL;
}

RC IndexManager::destroyFile(const string &fileName)
{
    RC err;
    string primary = fileName + PRIMARY_SUFFIX;
    string overflow = fileName + OVERFLOW_SUFFIX;

    if ((err = _pfm->destroyFile(primary.c_str())) != SUCCESSFUL) {
        return err;
    }
    if ((err = _pfm->destroyFile(overflow.c_str())) != SUCCESSFUL) {
        return err;
    }

	return SUCCESSFUL;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
    RC err;
    string primary = fileName + PRIMARY_SUFFIX;
    string overflow = fileName + OVERFLOW_SUFFIX;

    if ((err = _pfm->openFile(primary.c_str(), ixFileHandle._primaryHandle)) != SUCCESSFUL) {
        return err;
    }
    if ((err = _pfm->openFile(overflow.c_str(), ixFileHandle._overflowHandle)) != SUCCESSFUL) {
        return err;
    }

	return SUCCESSFUL;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    RC err;

    if ((err = _pfm->closeFile(ixfileHandle._primaryHandle)) != SUCCESSFUL) {
        return err;
    }
    if ((err = _pfm->closeFile(ixfileHandle._overflowHandle)) != SUCCESSFUL) {
        return err;
    }

    return SUCCESSFUL;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    RC err;

    KeyValue keyValue(key, attribute.type);
    MetadataPage metadata(ixfileHandle._overflowHandle);
    if (!metadata.isInitialized()) {
        return ERR_METADATA_MISSING;
    }

    // Count the bucket number
    unsigned bucket = calcBucketNumber(keyValue, metadata);

    // Check if the current bucket has been initialized, if not grow the bucket
    if (ixfileHandle._primaryHandle.getNumberOfPages() == 0) {
        if ((err = growToFit(ixfileHandle, metadata.getPrimaryPageCount(), attribute.type)) != SUCCESSFUL) {
            return err;
        }
    }

    // Load all pages within this bucket
    vector<DataPage *> cachedPages;
    loadBucketChain(cachedPages, ixfileHandle, bucket, attribute.type);

    // Check whether we need insert a new overflow page
    // If not, just insert the entry
    bool inserted = false;
    for (size_t i = 0; i < cachedPages.size(); i++) {
        DataPage *curPage = cachedPages[i];
        if (curPage->hasSpace(keyValue)) {
            if ((err = curPage->insert(keyValue, rid)) != SUCCESSFUL) {
                return err;
            } else {
                inserted = true;
                break;
            }
        }
    }
    if (!inserted) {
        // Add a new page.
        // Note that overflow page # starts from 1.
        unsigned overflowPageCount = metadata.getOverflowPageCount();
        DataPage *newPage = new DataPage(ixfileHandle._overflowHandle, OVERFLOW_PAGE,
                attribute.type, ++overflowPageCount, true);
        cachedPages.back()->setPageNum(overflowPageCount);
        cachedPages.push_back(newPage);
        metadata.setOverflowPageCount(overflowPageCount);
        if ((err = newPage->insert(keyValue, rid)) != SUCCESSFUL) {
            return err;
        }

        // Split bucket
        // 1. Update metadata and reserve new split bucket
        vector<DataPage *> newCache;
        unsigned p = metadata.getNextSplitBucket();
        unsigned n = metadata.getCurrentBucketCount();
        unsigned total = metadata.getPrimaryPageCount();
        unsigned from = p, to = p + n;  // two buckets we need to redistribute entries between
        if (++p == n) {
            p = 0;
            n = n << 1;
        }
        DataPage *newBucketPage = new DataPage(ixfileHandle._primaryHandle, PRIMARY_PAGE,
                attribute.type, total++, true);
        newCache.push_back(newBucketPage);
        metadata.setNextSplitBucket(p);
        metadata.setCurrentBucketCount(n);
        metadata.setPrimaryPageCount(total);
        // 2. Redistribute entries between two buckets
        rebalanceBetween(ixfileHandle, from, cachedPages, to, newCache, metadata);

        // flush new bucket
        flushBucketChain(newCache);
    }

    // flush split bucket
    flushBucketChain(cachedPages);

    // Update total entries count
    metadata.setEntryCount(metadata.getEntryCount() + 1);

	return SUCCESSFUL;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
	return 0;
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber)
{
	return -1;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages)
{
	return -1;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages)
{
	return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

void IndexManager::loadBucketChain(vector<DataPage *> &buf, IXFileHandle &ixfileHandle,
                   unsigned bucketNum, const AttrType &keyType) {
    DataPage *primary = new DataPage(ixfileHandle._primaryHandle, PRIMARY_PAGE,
                keyType, bucketNum, false);
    buf.push_back(primary);
    DataPage *curPage = primary;
    unsigned nextPageNum;
    while ((nextPageNum = curPage->getNextPageNum()) != PAGE_END) {
        DataPage *overflow = new DataPage(ixfileHandle._overflowHandle, OVERFLOW_PAGE,
                    keyType, nextPageNum, false);
        buf.push_back(overflow);
        curPage = overflow;
    }
}

void IndexManager::flushBucketChain(vector<DataPage *> &buf) {
    for (size_t i = 0; i < buf.size(); i++) {
        delete buf[i];
    }
}

RC IndexManager::rebalanceBetween(IXFileHandle &ixfileHandle, unsigned oldBucket, vector<DataPage *> &oldCache,
          unsigned newBucket, vector<DataPage *> &newCache, MetadataPage &metadata) {
    unsigned cur = 0;
    vector<DataPage *> updatedCache;

    // Create an empty old primary page
    DataPage *op = oldCache[cur];
    AttrType keyType = op->getKeyType();
    updatedCache.push_back(new DataPage(ixfileHandle._primaryHandle, PRIMARY_PAGE, keyType,
                        op->getPageNum(), true));

    // Redistribute data
    for (size_t i = 0; i < oldCache.size(); i++) {
        DataPage *curPage = oldCache[i];
        unsigned entriesCount = curPage->getEntriesCount();
        for (unsigned j = 0; j < entriesCount; j++) {
            KeyValue key;
            RID rid;
            curPage->keyAt(j, key);
            curPage->ridAt(j, rid);
            unsigned bucket = calcBucketNumber(key, metadata);
            if (bucket == oldBucket) {
                if (!updatedCache[cur]->hasSpace(key)) {
                    DataPage *dp = oldCache[++cur];   // Index should not be out of bound here
                    updatedCache.back()->setNextPageNum(dp->getPageNum());
                    updatedCache.push_back(new DataPage(ixfileHandle._overflowHandle, OVERFLOW_PAGE,
                                keyType, dp->getPageNum(), true));
                }
                updatedCache[cur]->insert(key, rid);
            } else if (bucket == newBucket) {
                if (!newCache.back()->hasSpace(key)) {
                    unsigned overflowPageCount = metadata.getOverflowPageCount();
                    newCache.push_back(new DataPage(ixfileHandle._overflowHandle, OVERFLOW_PAGE,
                                keyType, ++overflowPageCount, true));
                    metadata.setOverflowPageCount(overflowPageCount);
                }
                newCache.back()->insert(key, rid);
            } else {
                __trace();
                return ERR_BAD_PAGE;
            }
        }
        curPage->discard();
    }

    // Discard old cache using new cache instead
    flushBucketChain(oldCache);
    oldCache = updatedCache;

    return SUCCESSFUL;
}

RC IndexManager::growToFit(IXFileHandle &ixfileHandle, unsigned pageNum, const AttrType &keyType) {
    return -1;
}

unsigned IndexManager::calcBucketNumber(const KeyValue &keyValue, const MetadataPage &metadata) const {
    return 0;
}

// IX Scan Iterator implementations
IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}


IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return -1;
}

void IX_PrintError (RC rc)
{
}

// MetadataPage implementations
MetadataPage::MetadataPage(FileHandle &handle) : _fileHandle(handle), _dirty(false) {
    if (handle.getNumberOfPages() == 0) {
        _initialized = false;
    } else {
        _initialized = true;
        RC rc = load();
        assert(rc == SUCCESSFUL);
    }
}

MetadataPage::~MetadataPage() {
    if (_initialized && _dirty) {
        RC err = flush();
        assert(err == SUCCESSFUL);
    }
}

RC MetadataPage::initialize(const unsigned &numberOfPages) {
    _entryCount = 0;
    _primaryPageCount = numberOfPages;
    _overflowPageCount = 0;
    _currentBucketCount = numberOfPages;
    _nextSplitBucket = 0;
    _initialBucketCount = numberOfPages;

    _initialized = true;
    _dirty = true;

    return SUCCESSFUL;
}

RC MetadataPage::load() {
    RC err;
    char page[PAGE_SIZE];

    if ((err = _fileHandle.readPage(0, page)) != SUCCESSFUL) {
        return err;
    }

    int offset = 0;
    memcpy((char *) &_entryCount, page + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) &_primaryPageCount, page + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) &_overflowPageCount, page + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) &_currentBucketCount, page + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) &_nextSplitBucket, page + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) &_initialBucketCount, page + offset, sizeof(int));
    offset += sizeof(int);

    return SUCCESSFUL;
}

RC MetadataPage::flush() {
    RC err;
    char page[PAGE_SIZE];

    int offset = 0;
    memcpy(page + offset, (char *) &_entryCount, sizeof(int));
    offset += sizeof(int);
    memcpy(page + offset, (char *) &_primaryPageCount, sizeof(int));
    offset += sizeof(int);
    memcpy(page + offset, (char *) &_overflowPageCount, sizeof(int));
    offset += sizeof(int);
    memcpy(page + offset, (char *) &_currentBucketCount, sizeof(int));
    offset += sizeof(int);
    memcpy(page + offset, (char *) &_nextSplitBucket, sizeof(int));
    offset += sizeof(int);
    memcpy(page + offset, (char *) &_initialBucketCount, sizeof(int));
    offset += sizeof(int);

    if ((err = _fileHandle.writePage(0, page)) != SUCCESSFUL) {
        return err;
    }
    _dirty = false;

    return SUCCESSFUL;
}

unsigned MetadataPage::getEntryCount() {
    return _entryCount;
}

void MetadataPage::setEntryCount(unsigned entryCount) {
    _dirty = true;
    _entryCount = entryCount;
}

unsigned MetadataPage::getPrimaryPageCount() {
    return _primaryPageCount;
}

void MetadataPage::setPrimaryPageCount(unsigned primaryPageCount) {
    _dirty = true;
    _primaryPageCount = primaryPageCount;
}

unsigned MetadataPage::getOverflowPageCount() {
    return _overflowPageCount;
}

void MetadataPage::setOverflowPageCount(unsigned overflowPageCount) {
    _dirty = true;
    _overflowPageCount = overflowPageCount;
}

unsigned MetadataPage::getCurrentBucketCount() {
    return _currentBucketCount;
}

void MetadataPage::setCurrentBucketCount(unsigned currentBucketCount) {
    _dirty = true;
    _currentBucketCount = currentBucketCount;
}

unsigned MetadataPage::getNextSplitBucket() {
    return _nextSplitBucket;
}

void MetadataPage::setNextSplitBucket(unsigned nextSplitBucket) {
    _dirty = true;
    _nextSplitBucket = nextSplitBucket;
}

unsigned MetadataPage::getInitialBucketCount() {
    return _initialBucketCount;
}

bool MetadataPage::isInitialized() {
    return _initialized;
}

void MetadataPage::setInitialized(bool initialized) {
    _initialized = initialized;
}

// Implementations of class DataPage
DataPage::DataPage(FileHandle &fileHandle, PageType pageType, AttrType keyType, unsigned pageNum, bool newPage)
  : _fileHandle(fileHandle), _pageType(pageType), _keyType(keyType), _pageNum(pageNum), _dirty(false) {
    RC err;
    if (newPage) {
        initialize();
    } else {
        err = load();
        assert(err == SUCCESSFUL);
    }
}

DataPage::~DataPage() {
    RC err = flush();
    assert(err == SUCCESSFUL);
}

RC DataPage::initialize() {
    _entriesCount = 0;
    _entriesSize = 0;
    _nextPageNum = PAGE_END;
    _keys.clear();
    _rids.clear();
    _entryMap.clear();
    return SUCCESSFUL;
}

RC DataPage::load() {
    RC err;
    char page[PAGE_SIZE];

    if ((err = _fileHandle.readPage(_pageNum, page)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // load all information from the page
    loadMetadata(page);
    deserializeData(page);

    // build the hash map
    for (size_t i = 0; i < _keys.size(); i++) {
        _entryMap[_keys[i].toString()].push_back(i);
    }

    return SUCCESSFUL;
}

RC DataPage::flush() {
    if (!_dirty || _discarded) {
        return SUCCESSFUL;
    }

    RC err;
    char page[PAGE_SIZE];

    wireMetadata(page);
    serializeData(page);

    if ((err = _fileHandle.writePage(_pageNum, page)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    _dirty = false;
    return SUCCESSFUL;
}

void DataPage::discard() {
    _discarded = true;
}

RC DataPage::keyAt(unsigned index, KeyValue &key) {
    if (index >= _entriesCount) {
        return ERR_OUT_OF_BOUND;
    }

    key = _keys[index];
    return SUCCESSFUL;
}

RC DataPage::ridAt(unsigned index, RID &rid) {
    if (index >= _entriesCount) {
        return ERR_OUT_OF_BOUND;
    }

    rid = _rids[index];
    return SUCCESSFUL;
}

bool DataPage::hasSpace(KeyValue &key) {
    size_t esize = entrySize(key);
    return esize + _entriesSize < 6 * META_UNIT;
}

RC DataPage::insert(KeyValue &key, const RID &rid) {
    int idx = static_cast<int>(_keys.size());
    size_t esize = entrySize(key);
    if (!hasSpace(key)) {
        return ERR_NO_SPACE;
    }

    _keys.push_back(key);
    _rids.push_back(rid);
    _entryMap[key.toString()].push_back(idx);
    _entriesSize += esize;
    _entriesCount++;
    _dirty = true;
    return SUCCESSFUL;
}

RC DataPage::remove(KeyValue &key, const RID &rid) {
    vector<int> indexes;
    string keyStr = key.toString();
    if (_entryMap.count(keyStr) > 0) {
        indexes = _entryMap[keyStr];
    }

    bool found = false;
    int idx;
    for (size_t i = 0; i < indexes.size(); i++) {
        if (_rids[indexes[i]] == rid) {
            idx = static_cast<int>(i);
            found = true;
            break;
        }
    }
    if (!found) {
        return ERR_ENTRY_NOT_FOUND;
    }

    _keys.erase(_keys.begin() + indexes[idx]);
    _rids.erase(_rids.begin() + indexes[idx]);
    _entryMap[keyStr].erase(_entryMap[keyStr].begin() + idx);
    if (_entryMap[keyStr].size() == 0) {
        _entryMap.erase(keyStr);
    }

    size_t esize = entrySize(key);
    _entriesSize -= esize;
    _entriesCount--;
    _dirty = true;
    return SUCCESSFUL;
}

PageType DataPage::getPageType() {
    return _pageType;
}

void DataPage::setPageType(PageType pageType) {
    _dirty = true;
    _pageType = pageType;
}

AttrType DataPage::getKeyType() {
    return _keyType;
}

void DataPage::setKeyType(AttrType keyType) {
    _dirty = true;
    _keyType = keyType;
}

unsigned DataPage::getPageNum() {
    return _pageNum;
}

void DataPage::setPageNum(unsigned pageNum) {
    _dirty = true;
    _pageNum = pageNum;
}

unsigned DataPage::getEntriesCount() {
    return _entriesCount;
}

void DataPage::setEntriesCount(unsigned entriesCount) {
    _dirty = true;
    _entriesCount = entriesCount;
}

unsigned DataPage::getEntriesSize() {
    return _entriesSize;
}

void DataPage::setEntriesSize(unsigned entriesSize) {
    _entriesSize = entriesSize;
}

unsigned DataPage::getNextPageNum() {
    return _nextPageNum;
}

void DataPage::setNextPageNum(unsigned nextPageNum) {
    _nextPageNum = nextPageNum;
}

void DataPage::wireMetadata(void *page) {
    memcpy((char *) page + (PAGE_SIZE - META_UNIT), (char *) &_pageType, META_UNIT);
    memcpy((char *) page + (PAGE_SIZE - META_UNIT * 2), (char *) &_keyType, META_UNIT);
    memcpy((char *) page + (PAGE_SIZE - META_UNIT * 3), (char *) &_pageNum, META_UNIT);
    memcpy((char *) page + (PAGE_SIZE - META_UNIT * 4), (char *) &_entriesCount, META_UNIT);
    memcpy((char *) page + (PAGE_SIZE - META_UNIT * 5), (char *) &_entriesSize, META_UNIT);
    memcpy((char *) page + (PAGE_SIZE - META_UNIT * 6), (char *) &_nextPageNum, META_UNIT);
}

void DataPage::loadMetadata(void *page) {
    memcpy((char *) &_pageType, (char *) page + (PAGE_SIZE - META_UNIT), META_UNIT);
    memcpy((char *) &_keyType, (char *) page + (PAGE_SIZE - META_UNIT * 2), META_UNIT);
    memcpy((char *) &_pageNum, (char *) page + (PAGE_SIZE - META_UNIT * 3), META_UNIT);
    memcpy((char *) &_entriesCount, (char *) page + (PAGE_SIZE - META_UNIT * 4), META_UNIT);
    memcpy((char *) &_entriesSize, (char *) page + (PAGE_SIZE - META_UNIT * 5), META_UNIT);
    memcpy((char *) &_nextPageNum, (char *) page + (PAGE_SIZE - META_UNIT * 6), META_UNIT);
}

void DataPage::serializeData(void *page) {
    size_t offset = 0;

    // Here we assume that # of keys and RIDs are the same. (It should be!)
    for (size_t i = 0; i < _keys.size(); i++) {
        char buf[PAGE_SIZE];

        // key
        size_t keysize = _keys[i].size();
        _keys[i].getRaw(buf);
        memcpy((char *) page + offset, (char *) buf, keysize);
        offset += keysize;

        // RID
        RID rid = _rids[i];
        memcpy((char *) page + offset, (char *) &(rid.pageNum), sizeof(int));
        offset += sizeof(int);
        memcpy((char *) page + offset, (char *) &(rid.slotNum), sizeof(int));
        offset += sizeof(int);
    }

    if (offset > PAGE_SIZE - 6 * META_UNIT) {
        __trace();  // error case
    }
}

void DataPage::deserializeData(void *page) {
    size_t offset = 0;

    for (size_t i = 0; i < _entriesCount; i++) {
        char buf[PAGE_SIZE];

        // key
        switch (_keyType) {
        case TypeInt:
            memcpy((char *) &buf, (char *) page + offset, sizeof(int));
            offset += sizeof(int);
            break;
        case TypeReal:
            memcpy((char *) &buf, (char *) page + offset, sizeof(float));
            offset += sizeof(float);
            break;
        case TypeVarChar:
            size_t size;
            memcpy((char *) &size, (char *) page + offset, sizeof(int));
            assert(size < PAGE_SIZE);
            memcpy((char *) buf, (char *) page + offset, sizeof(int) + size);  // copy whole data
            offset += (sizeof(int) + size);
            break;
        default:
            __trace();
            break;
        }

        // rid
        RID rid;
        memcpy((char *) &(rid.pageNum), (char *) page + offset, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) &(rid.slotNum), (char *) page + offset, sizeof(int));
        offset += sizeof(int);

        _keys.push_back(KeyValue(buf, _keyType));
        _rids.push_back(rid);
    }
}

size_t DataPage::entrySize(KeyValue &key) {
    size_t keysize = key.size();
    return keysize + 2 * sizeof(int);
}

