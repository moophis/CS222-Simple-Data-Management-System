
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
    unsigned bucket = calcBucketNumber(keyValue, attribute, metadata);

    // Check if the current bucket has been initialized, if not grow the bucket
    if (ixfileHandle._primaryHandle.getNumberOfPages() == 0) {
        if ((err = growToFit(ixfileHandle, metadata.getPrimaryPageCount(), attribute.type)) != SUCCESSFUL) {
            return err;
        }
    }

    // Load all pages within this bucket
    vector<DataPage *> cachedPages;
    loadBucketChain(cachedPages, ixfileHandle, bucket, attribute.type);

    // Check whether the same <key, RID> pair already exists in the bucket
    for (size_t i = 0; i < cachedPages.size(); i++) {
        DataPage *curPage = cachedPages[i];
        if (curPage->doExist(keyValue, rid)) {
            return ERR_DUPLICATE_ENTRY;
        }
    }

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
        rebalanceBetween(ixfileHandle, from, cachedPages, to, newCache, metadata, attribute);

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
    RC err;

    KeyValue keyValue(key, attribute.type);
    MetadataPage metadata(ixfileHandle._overflowHandle);
    if (!metadata.isInitialized()) {
        return ERR_METADATA_MISSING;
    }

    // Count the bucket number
    unsigned bucket = calcBucketNumber(keyValue, attribute, metadata);

    // Check if the current bucket has been initialized, if not grow the bucket
    if (ixfileHandle._primaryHandle.getNumberOfPages() == 0) {
        if ((err = growToFit(ixfileHandle, metadata.getPrimaryPageCount(), attribute.type)) != SUCCESSFUL) {
            return err;
        }
    }

    // Load all pages within this bucket and delete
    bool deleted = false;
    vector<DataPage *> cachedPages;
    loadBucketChain(cachedPages, ixfileHandle, bucket, attribute.type);
    for (size_t i = 0; i < cachedPages.size(); i++) {
        DataPage *p = cachedPages[i];
        if ((err = p->remove(keyValue, rid)) == SUCCESSFUL) {
            deleted = true;
            break;
        }
        if (err != ERR_ENTRY_NOT_FOUND) {
            return err;
        }
    }

    if (!deleted) {
        return ERR_ENTRY_NOT_FOUND;
    }

    // Rearrange pages within a page once find an empty page
    bool emptyBucket = false;
    rebalanceWithin(ixfileHandle, bucket, cachedPages, emptyBucket, metadata);

    // Update metadata
    unsigned p = metadata.getNextSplitBucket();
    unsigned n = metadata.getCurrentBucketCount();
    unsigned total = metadata.getPrimaryPageCount();
    unsigned init = metadata.getInitialBucketCount();
    while (emptyBucket && bucket == total - 1 && total > init) {
        // Shrink the bucket
        total--;
        if (p == 0) {
            n >>= 1;
            p = n - 1;
        } else {
            p--;
        }
        // Continue shrinking if the new last bucket is still empty
        vector<DataPage *> cache;
        bucket = total - 1;
        loadBucketChain(cache, ixfileHandle, bucket, attribute.type);
        rebalanceWithin(ixfileHandle, bucket, cache, emptyBucket, metadata);
    }
    metadata.setNextSplitBucket(p);
    metadata.setCurrentBucketCount(n);
    metadata.setPrimaryPageCount(total);

    // flush bucket
    flushBucketChain(cachedPages);

    return SUCCESSFUL;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
    KeyValue keyVal(key, attribute.type);
	return keyVal.hashCode();
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber)
{
    RC err;
    MetadataPage metadata(ixfileHandle._overflowHandle);
    unsigned total = metadata.getPrimaryPageCount();
    unsigned entriesCount = 0;
    if (primaryPageNumber >= total) {
        return ERR_OUT_OF_BOUND;
    }
    vector<DataPage *> cachedPages;
    loadBucketChain(cachedPages, ixfileHandle, primaryPageNumber, attribute.type);

    // Print primary page first
    DataPage *primary = cachedPages[0];
    entriesCount += primary->getEntriesCount();
    cout << "Primary Page No. " << primary->getPageNum() << endl;
    if ((err = printEntries(primary)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // Print overflow pages
    bool lastPrimary = true;
    unsigned lastPage = primary->getPageNum();
    for (unsigned i = 1; i < cachedPages.size(); i++) {
        DataPage *overflow = cachedPages[i];
        cout << "Overflow Page No. " << overflow->getPageNum() << " lined to ";
        if (lastPrimary) {
            cout << "primary page " << lastPage << endl;
            lastPrimary = false;
        } else {
            cout << "overflow page " << lastPage << endl;
        }
        lastPage = overflow->getPageNum();
        if ((err = printEntries(overflow)) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }

    return SUCCESSFUL;
}

RC IndexManager::printEntries(DataPage *page) {
    RC err;

    cout << "\ta. # of entries: " << page->getEntriesCount() << endl;
    cout << "\tb. entries: ";
    for (unsigned i = 0; i < page->getEntriesCount(); i++) {
        KeyValue key;
        RID rid;
        if ((err = page->keyAt(i, key)) != SUCCESSFUL) {
            __trace();
            return err;
        }
        if ((err = page->ridAt(i, rid)) != SUCCESSFUL) {
            __trace();
            return err;
        }

        cout << "[" << key.toString() << "|" << rid.pageNum << ","
             << rid.slotNum << "] ";
    }
    cout << endl;

    return SUCCESSFUL;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages)
{
    MetadataPage metadata(ixfileHandle._overflowHandle);
    numberOfPrimaryPages = metadata.getPrimaryPageCount();

    return SUCCESSFUL;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages)
{
    MetadataPage metadata(ixfileHandle._overflowHandle);
    numberOfAllPages = metadata.getPrimaryPageCount();
    if (metadata.getOverflowPageCount() < metadata.getDelOverflowPageCount()) {
        return ERR_METADATA_ERROR;
    }
    numberOfAllPages += metadata.getOverflowPageCount() - metadata.getDelOverflowPageCount();
    numberOfAllPages++;     // metadata page

	return SUCCESSFUL;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool            lowKeyInclusive,
    bool            highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
    RC err;

    MetadataPage metadata(ixfileHandle._overflowHandle);
    if (!metadata.isInitialized()) {
        return ERR_METADATA_MISSING;
    }

    // Check if the current bucket has been initialized, if not grow the bucket
    if (ixfileHandle._primaryHandle.getNumberOfPages() == 0) {
        if ((err = growToFit(ixfileHandle, metadata.getPrimaryPageCount(), attribute.type)) != SUCCESSFUL) {
            return err;
        }
    }

    ix_ScanIterator._ixm = IndexManager::instance();
    ix_ScanIterator._active = true;
    ix_ScanIterator._ixFileHandle = ixfileHandle;
    ix_ScanIterator._lowInclusive = lowKeyInclusive;
    ix_ScanIterator._highInclusive = highKeyInclusive;
    ix_ScanIterator._hasLowerBound = (lowKey != nullptr);
    ix_ScanIterator._hasUpperBound = (highKey != nullptr);
    if (ix_ScanIterator._hasLowerBound) {
        ix_ScanIterator._lowKey = KeyValue(lowKey, attribute.type);
    }
    if (ix_ScanIterator._hasUpperBound) {
        ix_ScanIterator._highKey = KeyValue(lowKey, attribute.type);
    }
    ix_ScanIterator._keyType = attribute.type;
    if ((lowKeyInclusive == highKeyInclusive) && ix_ScanIterator._hasLowerBound
            && ix_ScanIterator._hasUpperBound
            && ix_ScanIterator._lowKey.compare(ix_ScanIterator._highKey)) {
        ix_ScanIterator._scanType = HASH_SCAN;
        ix_ScanIterator._curBucketNum = calcBucketNumber(ix_ScanIterator._lowKey, attribute, metadata);
        ix_ScanIterator._curPageIndex = 0;
        ix_ScanIterator._curHashIndex = 0;
    } else {
        ix_ScanIterator._scanType = RANGE_SCAN;
        ix_ScanIterator._curBucketNum = 0;
        ix_ScanIterator._curPageIndex = 0;
        ix_ScanIterator._curRangeIndex = 0;
    }
    ix_ScanIterator._totalBucketNum = metadata.getPrimaryPageCount();
    ix_ScanIterator._curBucket.clear();

    return SUCCESSFUL;
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
          unsigned newBucket, vector<DataPage *> &newCache, MetadataPage &metadata, const Attribute &attribute) {
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
            unsigned bucket = calcBucketNumber(key, attribute, metadata);
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
    // Count the deleted overflow pages
    unsigned deleted = metadata.getDelOverflowPageCount();
    deleted += oldCache.size() - updatedCache.size();
    metadata.setDelOverflowPageCount(deleted);
    flushBucketChain(oldCache);
    oldCache = updatedCache;

    return SUCCESSFUL;
}

RC IndexManager::rebalanceWithin(IXFileHandle &ixfileHandle, unsigned bucket,
            vector<DataPage *> &cache, bool &emptyBucket, MetadataPage &metadata) {
    unsigned deleted = 0;

    emptyBucket = false;
    if (cache.size() == 1 && cache[0]->getEntriesCount() == 0) {
        emptyBucket = true;
        return SUCCESSFUL;
    }
    if (cache.size() > 1) {
        DataPage *before = cache[0], *after = cache[1];
        if (before->getEntriesCount() == 0) {
            before->_keys = after->_keys;
            before->_rids = after->_rids;
            before->setNextPageNum(after->getNextPageNum());
            after->discard();
            deleted++;
        } else {
            for (size_t i = 1; i < cache.size(); i++) {
                if (cache[i]->getEntriesCount() == 0) {
                    cache[i-1]->setNextPageNum(cache[i]->getNextPageNum());
                    cache[i]->discard();
                    deleted++;
                    break;
                }
            }
        }
    }
    metadata.setDelOverflowPageCount(metadata.getDelOverflowPageCount() + deleted);

    return SUCCESSFUL;
}

RC IndexManager::growToFit(IXFileHandle &ixfileHandle, unsigned pageNum, const AttrType &keyType) {
    unsigned start = ixfileHandle._primaryHandle.getNumberOfPages();
    for (unsigned i = start; i < pageNum; i++) {
        DataPage dp(ixfileHandle._primaryHandle, PRIMARY_PAGE, (AttrType &) keyType, i, true);
    }

    return SUCCESSFUL;
}

unsigned IndexManager::calcBucketNumber(KeyValue &keyValue, const Attribute &attribute,
        MetadataPage &metadata) {
    char raw[PAGE_SIZE];
    keyValue.getRaw(raw);
    unsigned hashVal = hash(attribute, raw);
    unsigned p = metadata.getNextSplitBucket();
    unsigned n = metadata.getCurrentBucketCount();
    unsigned bucket = hashVal & (n - 1);
    if (bucket < p) {
        bucket = hashVal & ((n << 1) - 1);
    }
    return bucket;
}

// IX Scan Iterator implementations
IX_ScanIterator::IX_ScanIterator()
{
    this->_active = true;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (_scanType == HASH_SCAN) {
        return getNextHashMatch(rid, key);
    } else if (_scanType == RANGE_SCAN) {
        return getNextRangeMatch(rid, key);
    } else {
        return IX_EOF;
    }
}

RC IX_ScanIterator::close()
{
    this->_active = false;
	return SUCCESSFUL;
}

RC IX_ScanIterator::getNextHashMatch(RID &rid, void *key) {
    if (_curBucket.empty()) {
        _ixm->loadBucketChain(_curBucket, _ixFileHandle, _curBucketNum, _keyType);
    }

    while (_curPageIndex < _curBucket.size()) {
        DataPage *page = _curBucket[_curPageIndex];
        vector<int> indexes;
        page->findKeyIndexes(_lowKey, indexes);

        if (_curHashIndex >= indexes.size()) {
            _curHashIndex = 0;
            _curPageIndex++;
        } else {
            // found one entry
            _lowKey.getRaw(key);
            if (page->ridAt(indexes[_curHashIndex++], rid) != SUCCESSFUL) {
                // error should not happen
                __trace();
                return IX_EOF;
            }
            return SUCCESSFUL;
        }
    }

    _ixm->flushBucketChain(_curBucket);
    _curBucket.clear();
    return IX_EOF;
}

RC IX_ScanIterator::getNextRangeMatch(RID &rid, void *key) {
    while (_curBucketNum < _totalBucketNum) {
        if (_curBucket.empty()) {
            _ixm->loadBucketChain(_curBucket, _ixFileHandle, _curBucketNum, _keyType);
        }

        while (_curPageIndex < _curBucket.size()) {
            DataPage *page = _curBucket[_curPageIndex];
            unsigned count = page->getEntriesCount();
            bool found = false;
            for (; _curRangeIndex < count && !found; _curRangeIndex++) {
                KeyValue keyVal;
                // Fetch the key
                if (page->keyAt(_curRangeIndex, keyVal) != SUCCESSFUL) {
                    __trace();
                    return IX_EOF;
                }
                // Compare with lower bound
                if (_hasLowerBound) {
                    if ((_lowInclusive && _lowKey.compare(keyVal) > 0) ||
                            (!_lowInclusive && _lowKey.compare(keyVal) >= 0)) {
                        continue;
                    }
                }
                // Compare with upper bound
                if (_hasUpperBound) {
                    if ((_highInclusive && _highKey.compare(keyVal) < 0) ||
                            (!_highInclusive && _highKey.compare(keyVal) <= 0)) {
                        continue;
                    }
                }
                // OK now the key is within the range
                keyVal.getRaw(key);
                if (page->ridAt(_curRangeIndex, rid) != SUCCESSFUL) {
                    // error should not happen
                    __trace();
                    return IX_EOF;
                }
                found = true;
            }
            if (found) {
                return SUCCESSFUL;
            } else {
                _curPageIndex++;
            }
        }
        _ixm->flushBucketChain(_curBucket);
        _curBucket.clear();
        _curBucketNum++;
        _curPageIndex = 0;
    }

    return IX_EOF;
}

IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    unsigned rc = 0, wc = 0, ac = 0;
    readPageCount = writePageCount = appendPageCount = 0;
    _primaryHandle.collectCounterValues(rc, wc, ac);
    readPageCount += rc;
    writePageCount += wc;
    appendPageCount += ac;
    _overflowHandle.collectCounterValues(rc, wc, ac);
    readPageCount += rc;
    writePageCount += wc;
    appendPageCount += ac;
    return SUCCESSFUL;
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
    if (_initialized || _dirty) {
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
    memcpy((char *) &_delOverflowPageCount, page + offset, sizeof(int));
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
    memcpy(page + offset, (char *) &_delOverflowPageCount, sizeof(int));
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

unsigned MetadataPage::getDelOverflowPageCount() {
    return _delOverflowPageCount;
}

void MetadataPage::setDelOverflowPageCount(unsigned delOverflowPageCount) {
    _dirty = true;
    _delOverflowPageCount = delOverflowPageCount;
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
    _dirty = true;
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

RC DataPage::findKeyIndexes(KeyValue &key, vector<int> &indexes) {
    string keystr = key.toString();
    if (_entryMap.count(keystr) != 0) {
        indexes = _entryMap[keystr];
    }
    return SUCCESSFUL;
}

bool DataPage::hasSpace(KeyValue &key) {
    size_t esize = entrySize(key);
    return esize + _entriesSize < 6 * META_UNIT;
}

bool DataPage::doExist(KeyValue &key, const RID &rid) {
    string keystr = key.toString();
    if (_entryMap.count(keystr) != 0) {
        vector<int> &indexes = _entryMap[keystr];
        for (size_t i = 0; i < indexes.size(); i++) {
            if (_rids[indexes[i]] == rid) {
                return true;
            }
        }
    }

    return false;
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

