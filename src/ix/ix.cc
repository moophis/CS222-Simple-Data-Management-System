
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
        __trace();
        return ERR_INV_INIT_BUCKET;
    }

    if ((err = _pfm->createFile(primary.c_str())) != SUCCESSFUL) {
        __trace();
        return err;
    }
    if ((err = _pfm->createFile(overflow.c_str())) != SUCCESSFUL) {
        __trace();
        return err;
    }

    FileHandle h2;
    if ((err = _pfm->openFile(overflow.c_str(), h2)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    __trace();
    MetadataPage metadataPage(h2);
    metadataPage.initialize(numberOfPages);

    if ((err = metadataPage.flush()) != SUCCESSFUL) {
        __trace();
        return err;
    }

    if ((err = _pfm->closeFile(h2)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    __trace();
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
//    __trace();
//    cout << "bucket number: " << bucket << endl;

    // Check if the current bucket has been initialized, if not grow the bucket
    if (ixfileHandle._primaryHandle.getNumberOfPages() == 0) {
        if ((err = growToFit(ixfileHandle, metadata.getPrimaryPageCount(), attribute.type)) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }

//    __trace();
    // Load all pages within this bucket
    vector<DataPage *> cachedPages;
    loadBucketChain(cachedPages, ixfileHandle, bucket, attribute.type);

//    __trace();
    // Check whether the same <key, RID> pair already exists in the bucket
    for (size_t i = 0; i < cachedPages.size(); i++) {
        DataPage *curPage = cachedPages[i];
        if (curPage->doExist(keyValue, rid)) {
            return ERR_DUPLICATE_ENTRY;
        }
    }

//    __trace();
    // Check whether we need to insert a new overflow page
    // If not, just insert the entry
    bool inserted = false;
    if ((err = insertInternal(cachedPages, keyValue, rid, inserted)) != SUCCESSFUL) {
        __trace();
        return err;
    }

//    cout << "Inserted in existing pages? " << inserted << endl;
    if (!inserted) {
        // Debug
//        cout << "Last Page: PageType: " << cachedPages.back()->getPageType() << ", Num: "
//             << cachedPages.back()->getPageNum() << ", Next: " << cachedPages.back()->getNextPageNum() << endl;
//        cout << "Bucket # to insert: " << bucket << endl;

        // Split bucket
        // Update metadata
        vector<DataPage *> oldCache;
        vector<DataPage *> newCache;
        unsigned p = metadata.getNextSplitBucket();
        unsigned n = metadata.getCurrentBucketCount();
        unsigned total = metadata.getPrimaryPageCount();
        unsigned from = p, to = p + n;  // two buckets we need to redistribute entries between
        if (++p == n) {
            p = 0;
            n = n << 1;
        }
        total++;
        metadata.setNextSplitBucket(p);
        metadata.setCurrentBucketCount(n);
        metadata.setPrimaryPageCount(total);

        // Load bucket to be split and reserve new spill bucket.
        loadBucketChain(oldCache, ixfileHandle, from, attribute.type);
        DataPage *newBucketPage = new DataPage(ixfileHandle._primaryHandle, PRIMARY_PAGE,
                attribute.type, total - 1, true);
        newCache.push_back(newBucketPage);

        // Redistribute entries between two buckets
        if ((err = rebalanceBetween(ixfileHandle, from, oldCache, to,
                newCache, metadata, attribute)) != SUCCESSFUL) {
            __trace();
            return err;
        }

        // Check whether the bucket we split is the one we should insert entry into.
        if (from != bucket) {
            if ((err = appendInternal(cachedPages, keyValue, rid, metadata,
                    ixfileHandle, attribute)) != SUCCESSFUL) {
                __trace();
                return err;
            }
        } else {
            // Discard previous bucket cache
            for (unsigned i = 0; i < cachedPages.size(); i++) {
                cachedPages[i]->discard();
            }

            // Recalculate bucket number according to updated metadata
            unsigned bkt = calcBucketNumber(keyValue, attribute, metadata);
//            cout << "New bucket # to insert: " << bkt << endl;
            if (bkt == from) {
//                __trace();
                if ((err = insertIntoBucket(oldCache, keyValue, rid, metadata,
                        ixfileHandle, attribute)) != SUCCESSFUL) {
                    __trace();
                    return err;
                }
//                __trace();
            } else if (bkt == to) {
//                __trace();
                if ((err = insertIntoBucket(newCache, keyValue, rid, metadata,
                        ixfileHandle, attribute)) != SUCCESSFUL) {
                    __trace();
                    return err;
                }
//                __trace();
            } else {
                __trace();
                metadata.printMetadata();
                return ERR_BAD_PAGE;
            }
        }

//        __trace();
        // flush split and new bucket
        if ((err = flushBucketChain(oldCache)) != SUCCESSFUL) {
            __trace();
            return err;
        }
//        __trace();
        if ((err = flushBucketChain(newCache)) != SUCCESSFUL) {
            __trace();
            metadata.printMetadata();
            return err;
        }
//        __trace();
    }

    if ((err = flushBucketChain(cachedPages)) != SUCCESSFUL) {
        __trace();
        return err;
    }

//    __trace();
    // Update total entries count
    metadata.setEntryCount(metadata.getEntryCount() + 1);

//    __trace();
//    cout << "Inserted: " << keyValue.toString() << endl;
//    metadata.flush();
//    __trace();
//    printIndexEntriesInAPage(ixfileHandle, attribute, bucket);
    return SUCCESSFUL;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    RC err;

    KeyValue keyValue(key, attribute.type);
    MetadataPage metadata(ixfileHandle._overflowHandle);
    if (!metadata.isInitialized()) {
        __trace();
        return ERR_METADATA_MISSING;
    }

    // Count the bucket number
    unsigned bucket = calcBucketNumber(keyValue, attribute, metadata);

    // Check if the current bucket has been initialized, if not grow the bucket
    if (ixfileHandle._primaryHandle.getNumberOfPages() == 0) {
        if ((err = growToFit(ixfileHandle, metadata.getPrimaryPageCount(), attribute.type)) != SUCCESSFUL) {
            __trace();
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
            __trace();
            return err;
        }
    }

    if (!deleted) {
        __trace();
        return ERR_ENTRY_NOT_FOUND;
    }

    // Rearrange pages within a page once finding an empty page
    bool emptyBucket = false;
    rebalanceWithin(ixfileHandle, bucket, cachedPages, emptyBucket, metadata);

    // flush bucket
    if ((err = flushBucketChain(cachedPages)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // Update metadata and shrink buckets if possible
    unsigned p = metadata.getNextSplitBucket();
    unsigned n = metadata.getCurrentBucketCount();
    unsigned total = metadata.getPrimaryPageCount();
    unsigned init = metadata.getInitialBucketCount();
    for (unsigned i = total - 1; i >= init; i--) {
        vector<DataPage *> cache;
        loadBucketChain(cache, ixfileHandle, i, attribute.type);
        bool empty = isEmptyBucket(cache);
        if ((err = flushBucketChain(cache)) != SUCCESSFUL) {
            __trace();
            return err;
        }
        if (!empty) {
            break;
        } else {
            __trace();
            // Shrink the bucket
            total--;
            if (p == 0) {
                n >>= 1;
                p = n - 1;
            } else {
                p--;
            }
        }
    }

    metadata.setNextSplitBucket(p);
    metadata.setCurrentBucketCount(n);
    metadata.setPrimaryPageCount(total);
    metadata.setEntryCount(metadata.getEntryCount() - 1);

    return SUCCESSFUL;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
    KeyValue keyVal(key, attribute.type);
    return keyVal.hashCode();
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber)
{
//    __trace();
    RC err;
    MetadataPage metadata(ixfileHandle._overflowHandle);
    unsigned total = metadata.getPrimaryPageCount();
    if (primaryPageNumber >= total) {
        return ERR_OUT_OF_BOUND;
    }
    vector<DataPage *> cachedPages;
    loadBucketChain(cachedPages, ixfileHandle, primaryPageNumber, attribute.type);

    // Total entries
    cout << "Number of total entries: " << metadata.getEntryCount() << endl;

    // Print primary page
    DataPage *primary = cachedPages[0];
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
        __trace();
        cout << "Overflow page #: " << metadata.getOverflowPageCount()
             << " Deleted #: " << metadata.getDelOverflowPageCount() << endl;
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
        ix_ScanIterator._highKey = KeyValue(highKey, attribute.type);
    }
    ix_ScanIterator._keyType = attribute.type;
    if ((lowKeyInclusive == highKeyInclusive) && ix_ScanIterator._hasLowerBound
            && ix_ScanIterator._hasUpperBound
            && ix_ScanIterator._lowKey.compare(ix_ScanIterator._highKey) == 0) {
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
//    __trace();
//    cout << "Load bucket: " << bucketNum << endl;
    DataPage *primary = new DataPage(ixfileHandle._primaryHandle, PRIMARY_PAGE,
                keyType, bucketNum, false);
    buf.push_back(primary);
//    cout << "\tPushed a primary page: pageNum: " << bucketNum << endl;
    DataPage *curPage = primary;
    unsigned nextPageNum = PAGE_END;
    while ((nextPageNum = curPage->getNextPageNum()) != PAGE_END) {
//        cout << "\tPushed an overflow page: pageNum: " << nextPageNum;
        DataPage *overflow = new DataPage(ixfileHandle._overflowHandle, OVERFLOW_PAGE,
                    keyType, nextPageNum, false);
        buf.push_back(overflow);
        curPage = overflow;
//        cout << " (Pushed)" << endl;
    }
}

RC IndexManager::flushBucketChain(vector<DataPage *> &buf) {
//    __trace();
//    cout << "We have " << buf.size() << " page(s) to flush" << endl;
    RC err;
    for (size_t i = 0; i < buf.size(); i++) {
//        buf[i]->printMetadata();
        if ((err = buf[i]->flush()) != SUCCESSFUL) {
            __trace();
            cout << "Error: flush page (i = " << i << " of " << buf.size() << ")" << endl;
            cout << "Dump the whole bucket: " << endl;
            for (size_t j = 0; j < buf.size(); j++) {
                cout << "*** i = " << j << endl;
                buf[j]->printMetadata();
            }
            return err;
        }
        delete buf[i];
    }
    return SUCCESSFUL;
}

RC IndexManager::rebalanceBetween(IXFileHandle &ixfileHandle, unsigned oldBucket, vector<DataPage *> &oldCache,
          unsigned newBucket, vector<DataPage *> &newCache, MetadataPage &metadata, const Attribute &attribute) {
//    __trace();
//    cout << "Old bucket #" << oldBucket << ", newBucket #" << newBucket << endl;
    RC err;
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
//        cout << "PageType: " << curPage->getPageType() << ", pageNum: "
//             << curPage->getPageNum() << endl;
        unsigned entriesCount = curPage->getEntriesCount();
        for (unsigned j = 0; j < entriesCount; j++) {
            KeyValue key;
            RID rid;
            curPage->keyAt(j, key);
            curPage->ridAt(j, rid);
            unsigned bucket = calcBucketNumber(key, attribute, metadata);
//            cout << "key: " << key.toString() << ", hash: " << key.hashCode() << " new: " << bucket << endl;
//            cout << " ** [" << key.toString() << "|" << rid.pageNum
//                 << "," << rid.slotNum << "] redistribute to " << bucket << endl;
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
                    DataPage *dp = new DataPage(ixfileHandle._overflowHandle, OVERFLOW_PAGE,
                            keyType, ++overflowPageCount, true);
                    newCache.back()->setNextPageNum(dp->getPageNum());
                    newCache.push_back(dp);
                    metadata.setOverflowPageCount(overflowPageCount);
                }
                newCache.back()->insert(key, rid);
            } else {
                __trace();
                metadata.printMetadata();
                cout << "New bucket: " << bucket << endl;
                cout << "key: " << key.toString() << ", hash: " << key.hashCode() << endl;
                curPage->printMetadata();
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
    if ((err = flushBucketChain(oldCache)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    oldCache = updatedCache;

//    __trace();
    return SUCCESSFUL;
}

RC IndexManager::rebalanceWithin(IXFileHandle &ixfileHandle, unsigned bucket,
            vector<DataPage *> &cache, bool &emptyBucket, MetadataPage &metadata) {
    unsigned deleted = 0;

    // Check whether the bucket is empty
    emptyBucket = isEmptyBucket(cache);
    if (emptyBucket) {
        __trace();
        return SUCCESSFUL;
    }

    // Redistribute data
    if (cache.size() > 1) {
        DataPage *before = cache[0], *after = cache[1];
        if (before->getEntriesCount() == 0) {
            __trace();
//            cout << "Before rebalance: before->next: " << before->getNextPageNum()
//                 << ", after->count: " << after->getEntriesCount() << endl;

            before->_keys = after->_keys;
            before->_rids = after->_rids;
            before->setNextPageNum(after->getNextPageNum());
            before->setEntriesCount(after->getEntriesCount());
            before->setEntriesSize(after->getEntriesSize());
            after->discard();
            deleted++;

//            cout << "After rebalance: before->next: " << before->getNextPageNum()
//                 << ", before->count: " << before->getEntriesCount() << endl;
        } else {
            for (size_t i = 1; i < cache.size(); i++) {
                if (cache[i]->getEntriesCount() == 0) {
//                    __trace();
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

RC IndexManager::insertInternal(vector<DataPage *> &cachedPages, KeyValue &keyValue,
          const RID &rid, bool &inserted) {
    RC err;

    inserted = false;
    for (size_t i = 0; i < cachedPages.size(); i++) {
        DataPage *curPage = cachedPages[i];
//        cout << "+ i = " << i << " Cached Page #" << curPage->getPageNum() << endl;
        if (curPage->hasSpace(keyValue)) {
            if ((err = curPage->insert(keyValue, rid)) != SUCCESSFUL) {
                return err;
            } else {
                inserted = true;
                break;
            }
        }
    }
    return SUCCESSFUL;
}

RC IndexManager::appendInternal(vector<DataPage *> &cachedPages, KeyValue &keyValue,
        const RID &rid, MetadataPage &metadata, IXFileHandle &ixfileHandle,
        const Attribute &attribute) {
    RC err;
    unsigned overflowPageCount = metadata.getOverflowPageCount();
    DataPage *newPage = new DataPage(ixfileHandle._overflowHandle, OVERFLOW_PAGE,
            attribute.type, ++overflowPageCount, true);

    cachedPages.back()->setNextPageNum(overflowPageCount);
    cachedPages.push_back(newPage);
    metadata.setOverflowPageCount(overflowPageCount);
    if ((err = newPage->insert(keyValue, rid)) != SUCCESSFUL) {
        return err;
    }

    return SUCCESSFUL;
}

RC IndexManager::insertIntoBucket(vector<DataPage *> &cachedPages, KeyValue &keyValue,
          const RID &rid, MetadataPage &metadata, IXFileHandle &ixfileHandle,
          const Attribute &attribute) {
    RC err;

    bool ins = false;
    if ((err = insertInternal(cachedPages, keyValue, rid, ins)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    if (!ins) {
        if ((err = appendInternal(cachedPages, keyValue, rid, metadata,
                ixfileHandle, attribute)) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }

    return SUCCESSFUL;
}

bool IndexManager::isEmptyBucket(vector<DataPage *> &cache) {
    for (unsigned i = 0; i < cache.size(); i++) {
        if (cache[i]->getEntriesCount() != 0) {
            return false;
        }
        if (cache[i]->getNextPageNum() == PAGE_END) {
            return true;
        }
    }
    __trace();   // Not reach (pages should end with PAGE_END)
    return true;
}

RC IndexManager::growToFit(IXFileHandle &ixfileHandle, unsigned pageNum, const AttrType &keyType) {
    unsigned start = ixfileHandle._primaryHandle.getNumberOfPages();
//    __trace();
//    cout << "start: " << start << " pageNum: " << pageNum << endl;
    RC err;
    for (unsigned i = start; i < pageNum; i++) {
//        cout << "Initializing " << i << endl;
        DataPage dp(ixfileHandle._primaryHandle, PRIMARY_PAGE, (AttrType &) keyType, i, true);
        if ((err = dp.flush()) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }

    return SUCCESSFUL;
}

unsigned IndexManager::calcBucketNumber(KeyValue &keyValue, const Attribute &attribute,
        MetadataPage &metadata) {
    unsigned hashVal = keyValue.hashCode();
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
    RC err;
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

    if ((err = _ixm->flushBucketChain(_curBucket)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    _curBucket.clear();
    return IX_EOF;
}

RC IX_ScanIterator::getNextRangeMatch(RID &rid, void *key) {
    RC err;

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
                _curRangeIndex = 0;
            }
        }
        if ((err = _ixm->flushBucketChain(_curBucket)) != SUCCESSFUL) {
            __trace();
            return err;
        }
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
//    __trace();
//    cout << "Primary: rc " << rc << " wc " << wc << " ac " << ac << endl;
    readPageCount += rc;
    writePageCount += wc;
    appendPageCount += ac;
    _overflowHandle.collectCounterValues(rc, wc, ac);
//    cout << "Overflow: rc " << rc << " wc " << wc << " ac " << ac << endl;
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
//    __trace();
    if (_fileHandle.getNumberOfPages() == 0) {
//        __trace();
        _initialized = false;
    } else {
        _initialized = true;
        RC rc = load();
        assert(rc == SUCCESSFUL);
    }
//    __trace();
}

MetadataPage::~MetadataPage() {
//    __trace();
    if (_dirty) {
        RC err = flush();
        assert(err == SUCCESSFUL);
    }
//    __trace();
}

RC MetadataPage::initialize(const unsigned &numberOfPages) {
    if (_initialized) {
        return ERR_INV_OPERATION;
    }

    _entryCount = 0;
    _primaryPageCount = numberOfPages;
    _overflowPageCount = 0;
    _delOverflowPageCount = 0;
    _currentBucketCount = numberOfPages;
    _nextSplitBucket = 0;
    _initialBucketCount = numberOfPages;

    _initialized = true;
    _dirty = true;

    return SUCCESSFUL;
}

RC MetadataPage::load() {
//    __trace();
    RC err;
    char page[PAGE_SIZE];

    if ((err = _fileHandle.readPage(0, page)) != SUCCESSFUL) {
        __trace();
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

//    printMetadata();

    return SUCCESSFUL;
}

RC MetadataPage::flush() {
    if (_dirty) {
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
            __trace();
            return err;
        }
        _dirty = false;
    }

    return SUCCESSFUL;
}

void MetadataPage::printMetadata() {
    cout << "===== Metadata =====" << endl;
    cout << "_entryCount: " << _entryCount << endl;
    cout << "_primaryPageCount: " << _primaryPageCount << endl;
    cout << "_overflowPageCount: " << _overflowPageCount << endl;
    cout << "_delOverflowPageCount: " << _delOverflowPageCount << endl;
    cout << "_currentBucketCount: " << _currentBucketCount << endl;
    cout << "_nextSplitBucket: " << _nextSplitBucket << endl;
    cout << "_initialBucketCount: " << _initialBucketCount << endl;
    cout << "====================" << endl;
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
    _dirty = true;
    _initialized = initialized;
}

// Implementations of class DataPage
DataPage::DataPage(FileHandle &fileHandle, PageType pageType, AttrType keyType, unsigned pageNum, bool newPage)
  : _fileHandle(fileHandle), _pageType(pageType), _keyType(keyType), _pageNum(pageNum), _dirty(false), _discarded(false) {
    RC err;
//    __trace();
    if (newPage) {
//        __trace();
        initialize();
    } else {
//        __trace();
        err = load();
        assert(err == SUCCESSFUL);
    }
//    __trace();
}

DataPage::~DataPage() {
//    __trace();
    RC err = flush();
    assert(err == SUCCESSFUL);
//    __trace();
}

RC DataPage::initialize() {
    _entriesCount = 0;
    _entriesSize = 0;
    _nextPageNum = PAGE_END;
    _keys.clear();
    _rids.clear();
    _entryMap.clear();
    _dirty = true;
    _discarded = false;
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

//    printMetadata();

    return SUCCESSFUL;
}

RC DataPage::flush() {
    if (!_dirty || _discarded) {
//        __trace();
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
//    __trace();
    size_t esize = entrySize(key);
//    cout << "\tCurrentEntrySize: " << _entriesSize << ", new: " << esize << endl;
    return esize + _entriesSize < PAGE_SIZE - 6 * META_UNIT;
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

void DataPage::printMetadata() {
    cout << "===== DataPage =====" << endl;
    cout << "_pageType: " << _pageType << endl;
    cout << "_keyType: " << _keyType << endl;
    cout << "_pageNum: " << _pageNum << endl;
    cout << "_entriesCount: " << _entriesCount << endl;
    cout << "_entriesSize: " << _entriesSize << endl;
    cout << "_nextPageNum: " << _nextPageNum << endl;
    cout << "====================" << endl;
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
    _dirty = true;
    _entriesSize = entriesSize;
}

unsigned DataPage::getNextPageNum() {
    return _nextPageNum;
}

void DataPage::setNextPageNum(unsigned nextPageNum) {
    _dirty = true;
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
            int size;
            memcpy((char *) &size, (char *) page + offset, sizeof(int));
            if (size < 0 || size > PAGE_SIZE) {
                __trace();
                cout << "Invalid size: " << size << " at i = " << i << endl;
            }
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

