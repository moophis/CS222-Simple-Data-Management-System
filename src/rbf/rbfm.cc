#include <iostream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <map>
#include <set>
//#include <unordered_map>
//#include <unordered_set>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

PagedFileManager *RecordBasedFileManager::_pfm_manager;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    _pfm_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

/**
 * Create a file.
 *
 * @param fileName
 *          the name of the file to be created.
 * @return status
 */
RC RecordBasedFileManager::createFile(const string &fileName) {
    return _pfm_manager->createFile(fileName.c_str());
}

/**
 * Delete a file.
 *
 * @param fileName
 *          the name of the file to be destroyed.
 * @return status
 */
RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pfm_manager->destroyFile(fileName.c_str());
}

/**
 * Open a file.
 *
 * @param fileName
 *          the name of the file to be opened.
 * @param fileHandle
 *          the file handle associated with this file.
 * @return status
 */
RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    RC err;
    // Note that _pfm_manager->openFile() will initialize fileHandle
    if ((err = _pfm_manager->openFile(fileName.c_str(), fileHandle)) != SUCCESSFUL) {
        return err;
    }
    if ((err = SpaceManager::instance()->bufferSizeInfo(fileName, fileHandle)) != SUCCESSFUL) {
        return err;
    }
    return SUCCESSFUL;
}

/**
 * Close a file.
 *
 * @param fileName
 *          the name of the file to be closed.
 * @return status
 */
RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    string fileName(fileHandle.getFileName());
    SpaceManager::instance()->clearFreeSpaceMap(fileName);
    return _pfm_manager->closeFile(fileHandle);
}

/**
 * Given a record descriptor, insert a record into a given file identified by the provided handle.
 *
 * @param fileHandle
 *          the provided file handle.
 * @param recordDescriptor
 *          the record descriptor.
 * @param data
 *          the record data to be inserted.
 * @param rid
 *          the record ID which is used to uniquely identify records in a file.
 * @param status
 */
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         fileHandle.getFileName() == NULL) {
        return ERR_BAD_HANDLE;
    }

    RC err = 0;
    string fileName(fileHandle.getFileName());

    // calculate the size of the record
    unsigned recordSize;
    if ((err = countRecordSize(recordDescriptor, data, recordSize)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // find and allocate a fit space to store the record: get page # and start position
    // if cannot find an existing page then append and initialize a new page
    int pageNum = -1;
    if ((err = SpaceManager::instance()->allocateSpace(fileName, fileHandle, recordSize, pageNum)) != SUCCESSFUL) {
        return err;
    }

    void *page = SpaceManager::getPageBuffer();
    if (pageNum == -1) {
        // need to append a new page
        SpaceManager::instance()->initCleanPage(page);
        pageNum = fileHandle.getNumberOfPages();
        SpaceManager::instance()->writeRecord(page, data, 0, recordSize);  // wire record data
        SpaceManager::instance()->setFreePtr(page, recordSize);           // update free pointer
        SpaceManager::instance()->setSlotCount(page, 1);                 // update # of slots
        SpaceManager::instance()->setSlot(page, 0, 0, recordSize);        // set start position of first slot

        // append that page
        if ((err = fileHandle.appendPage(page)) != SUCCESSFUL) {
            return err;
        }

        // update metadata, we need reserve one more slot since we don't have free existing ones
        int freeSize = PAGE_SIZE - recordSize - SpaceManager::instance()->getMetadataSize(2);  // existing + reserved one
        SpaceManager::instance()->updateFreeSpaceMap(fileName, pageNum, freeSize);

        // update RID
        rid.pageNum = (unsigned) pageNum;
        rid.slotNum = 0;
    } else {
        // use existing page, read that page
        if ((err = fileHandle.readPage(pageNum, page)) != SUCCESSFUL) {
            cout << "pageNum: " << pageNum << endl;
            __trace();
            return err;
        }

        // find place and insert record
        unsigned short start = SpaceManager::instance()->getFreePtr(page);
        SpaceManager::instance()->writeRecord(page, data, start, recordSize);

        // update metadata (in disk file and the map in memory)
        unsigned short slotCount = SpaceManager::instance()->getSlotCount(page);
        unsigned short firstFreeSlot;

        // check whether we can reused previously released slot
        int reservedSlotSpace = 0;
        if (!SpaceManager::instance()->hasFreeExistingSlot(page, slotCount, firstFreeSlot)) {
            // insert a new slot
            firstFreeSlot = slotCount;
            SpaceManager::instance()->setSlot(page, slotCount, start, recordSize);
            SpaceManager::instance()->setSlotCount(page, ++slotCount);
            // since we don't have free existing free slot, we need to reserve one for next insertion
            reservedSlotSpace = 1;
        } else {
            SpaceManager::instance()->setSlot(page, firstFreeSlot, start, recordSize);
        }

        // update free pointer
        SpaceManager::instance()->setFreePtr(page, start + recordSize);

        // write back the page
        if ((err = fileHandle.writePage(pageNum, page)) != SUCCESSFUL) {
            return err;
        }

        // update metadata
        int freeSize = PAGE_SIZE - start - recordSize
                        - SpaceManager::instance()->getMetadataSize(slotCount + reservedSlotSpace);
        SpaceManager::instance()->updateFreeSpaceMap(fileName, pageNum, freeSize);

        // Update RID
        rid.pageNum = (unsigned) pageNum;
        rid.slotNum = firstFreeSlot;
    }

//    SpaceManager::instance()->printFreeSpaceMap();
    return SUCCESSFUL;
}

/**
 * Given a record descriptor and RID, get the record.
 *
 * @param fileHandle
 * @param recordDescriptor
 * @param rid
 * @param data
 *          the record data to be stored.
 * @return status
 */
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
//    cout << "RID: page # " << rid.pageNum << ", slot # " << rid.slotNum << endl;
    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         fileHandle.getFileName() == NULL) {
        return ERR_BAD_HANDLE;
    }

    void *page = SpaceManager::getPageBuffer();
    unsigned pageCount = fileHandle.getNumberOfPages();
    if (rid.pageNum >= pageCount) {
        return ERR_RECORD_NOT_FOUND;
    }

    fileHandle.readPage(rid.pageNum, page);
    unsigned slotCount = SpaceManager::instance()->getSlotCount(page);
    if (rid.slotNum >= slotCount) {
        return ERR_RECORD_NOT_FOUND;
    }

    short startPos = SpaceManager::instance()->getSlotStartPos(page, rid.slotNum);
    unsigned short recordLength = SpaceManager::instance()->getSlotLength(page, rid.slotNum);
    if (startPos < 0 || startPos >= SpaceManager::instance()->getFreePtr(page) || recordLength > PAGE_SIZE) {
        return ERR_BAD_DATA;
    }

    // now read record
    SpaceManager::instance()->readRecord(page, data, startPos, recordLength);

    return SUCCESSFUL;
}

/**
 * Print the record data given its schema.
 *
 * @param recordDescriptor
 *          the record descriptor.
 * @param data
 *          the record data to be printed out.
 * @return status
 */
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int offset = 0;

    std::cout << "======================" << std::endl;
    std::cout << "Size of record desc: " << recordDescriptor.size() << endl;
    for (size_t i = 0; i < recordDescriptor.size(); i++) {
        Attribute attr = recordDescriptor[i];
        string name = attr.name;
        AttrType type = attr.type;
        AttrLength length = attr.length;

        if (type == TypeVarChar) {
            // read string length
            AttrLength len = 0;
            memcpy((char *)&len, (char *)data + offset, sizeof(int));
            if (len > length) {
                return ERR_FORMAT;
            }
            offset += sizeof(int);

            // read the string
            void *buffer = malloc(len + 1);
            memcpy((char *)buffer, (char *)data + offset, len);
            std::cout << name << "(string = " << len << "): ";
            for (unsigned j = 0; j < len; j++) {
                std::cout << *((char *) buffer + j);
            }
            std::cout << " --> (origin): ";
            for (unsigned j = 0; j < len; j++) {
                std::cout << std::hex << (int)*((char *) buffer + j) << " ";
            }
            std::cout << std::dec << std::endl;
            free(buffer);
            offset += len;
        } else if (type == TypeInt) {
            int num;
            memcpy((char *)&num, (char *)data + offset, sizeof(int));
            std::cout << name << "(int): " << num;
            std::cout << " --> (origin): ";
            for (unsigned j = 0; j < sizeof(int); j++) {
                std::cout << std::hex << (int)*((char *)&num + j) << " ";
            }
            std::cout << std::dec << std::endl;
            offset += sizeof(int);
        } else if (type == TypeReal) {
            float real;
            memcpy((char *)&real, (char *)data + offset, sizeof(float));
            std::cout << name << "(real): " << real;
            std::cout << " --> (origin): ";
            for (unsigned j = 0; j < sizeof(int); j++) {
                std::cout << std::hex << (int)*((char *)&real + j) << " ";
            }
            std::cout << std::dec << std::endl;
            offset += sizeof(float);
        } else {
            return ERR_UNKNOWN_TYPE;
        }
    }
    std::cout << "======================" << std::endl;

    return SUCCESSFUL;
}

/**
 * Count the size of the record
 */
RC RecordBasedFileManager::countRecordSize(const vector<Attribute> &recordDescriptor, const void *data, unsigned &size) {
    unsigned offset = 0;
    for (size_t i = 0; i < recordDescriptor.size(); i++) {
        Attribute attr = recordDescriptor[i];
        string name = attr.name;
        AttrType type = attr.type;
        AttrLength length = attr.length;

        switch (type) {
        case TypeVarChar:{
            AttrLength len = 0;
            memcpy((char *)&len, (char *)data + offset, sizeof(int));
            if (len > length) {
                cout << "countRecordSize(): string len = " << len << ", should be " << length << endl;
                return ERR_FORMAT;
            }
            offset += sizeof(int);  // length itself
            offset += len;          // string length
            break;
        }
        case TypeInt:
            offset += sizeof(int);
            break;
        case TypeReal:
            offset += sizeof(float);
            break;
        default:
            return ERR_UNKNOWN_TYPE;
        }
    }

    size = offset;
    return SUCCESSFUL;
}

// XXX: not tested
/**
 * Delete all records in the file.
 */
RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle) {
    string fileName(fileHandle.getFileName());
    return SpaceManager::instance()->deallocateAllSpaces(fileName, fileHandle);
}

// XXX: not tested
/**
 * Given a record descriptor, delete the record identified by the given rid.
 */
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    string fileName(fileHandle.getFileName());
    return SpaceManager::instance()->deallocateSpace(fileName, fileHandle, rid.pageNum, rid.slotNum);
}

// TODO
/**
 * Given a record descriptor, update the record identified by the given rid with the passed data.
 * If the record grows and there is no space in the page to store the record, the record is migrated
 * to a new page with enough free space.
 */
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    return -1;
}

// TODO
/**
 * Given a record descriptor, read a specific attribute of a record identified by a given rid.
 */
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid,
        const string attributeName, void *data) {
    return -1;
}

// TODO
/**
 * Given a record descriptor, reorganize a page, i.e., push the free space towards the end of the page.
 */
RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber) {
    return -1;
}

// TODO
/**
 * Given a record descriptor, scan a file, i.e., sequentially read all the entries in the file.
 */
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator) {
    return -1;
}

// TODO: extra credit
/**
 * Given a record descriptor, reorganize the file which causes reorganization of the records such that
 * the records are collected towards the beginning of the file. Also, record redirection is eliminated.
 *
 * (In this case, and only this case, it is okay for rids to change.)
 */
RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor) {
    return -1;
}

/**
 * Space Manager Implementations
 */
typedef map<int, set<int> > FreeSpaceMap;    // <free space size, corresponding locations (page #)>

map<string, FreeSpaceMap> SpaceManager::__freeSpace;

SpaceManager* SpaceManager::_sp_manager = 0;

void *SpaceManager::__buffer = 0;

SpaceManager::SpaceManager() {

}

SpaceManager::~SpaceManager() {

}

SpaceManager* SpaceManager::instance() {
    if (_sp_manager == NULL) {
        _sp_manager = new SpaceManager();
    }
    return _sp_manager;
}

/**
 * Get the common buffer of size PAGE_SIZE in order to
 * reduce the memory cost.
 *
 * @return pointer to the buffer, NULL if no memory able to be allocated.
 */
void *SpaceManager::getPageBuffer() {
    if (!__buffer) {
        __buffer = malloc(PAGE_SIZE);
    }
    return __buffer;
}

/**
 * Collect and buffer the information about the free space from existing file
 * on the disk. Designed to be called by RecordBasedFileManager::openFile()
 *
 * @param fileName
 * @param fileHandle
 * @return status
 */
RC SpaceManager::bufferSizeInfo(const string &fileName, FileHandle &fileHandle) {
    clearFreeSpaceMap(fileName);

    RC err = 0;
    int pageNum = fileHandle.getNumberOfPages();
    for (int i = 0; i < pageNum; i++) {
//        cout << "Searching page " << i << endl;
        void *buffer = getPageBuffer();
        if (!buffer) {
            return ERR_NULLPTR;
        }

        if ((err = fileHandle.readPage(i, buffer)) != SUCCESSFUL) {
            __trace();
            return err;
        }

        /*
         * Extract metadata from the buffer.
         * Note: if all slots are all occupied with valid record, one
         * more slot metadata (4 byte) should be reserved. This will
         * reflect in the calculation of free space.
         */
        unsigned short freePtr = getFreePtr(buffer);
        unsigned short slotCount = getSlotCount(buffer);
        if (freePtr >= PAGE_SIZE - 4) {
            __trace();
            return ERR_BAD_DATA;
        }
        if (slotCount > PAGE_SIZE / 4) {
            __trace();
            return ERR_BAD_DATA;
        }

//        cout << "Free ptr: " << freePtr << ", slotCount: " << slotCount << endl;

        unsigned short firstSlot;
        if (!hasFreeExistingSlot(buffer, slotCount, firstSlot)) {
            slotCount++;
        } else {
            __trace();
        }

        int freeSize = PAGE_SIZE - freePtr - getMetadataSize(slotCount);
//        cout << "freeSize " << freeSize << endl;
        updateFreeSpaceMap(fileName, i, freeSize);
    }

//    printFreeSpaceMap();
    return SUCCESSFUL;
}

/**
 * Allocate new space for a new record to be inserted. Given space
 * required, return the position of the record to be inserted into
 * and update in-memory meta-info.
 *
 * @param fileName
 * @param fileHandle
 * @param spaceSize
 *          the size of the space trying to claim.
 * @param pageNum
 *          (return) the page number of the allocated space, -1 if cannot find such space.
 * @return status
 */
RC SpaceManager::allocateSpace(const string &fileName, FileHandle &fileHandle, int spaceSize, int &pageNum) {
//    cout << "In SpaceManager::allocateSpace, fileName: " << fileName << " space request: " << spaceSize << endl;
    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         strcmp(fileName.c_str(), fileHandle.getFileName()) != 0) {
        __trace();
        return ERR_BAD_HANDLE;
    }

    if (spaceSize >= PAGE_SIZE - FREE_PTR_LEN - SLOT_NUM_LEN - SLOT_START_LEN - SLOT_LEN_LEN) {
        // 8 = # of entries (2 byte) + free chuck header pointer (2 byte)
        // + information for the first slot (4 byte)
        return ERR_SIZE_TOO_LARGE;
    }

    /*
     * Retrieve suitable free page according to metadata
     * Note: the free space map will also delete the map entry of allocated space
     * and clean zero length entry if possible.
     */
    int page = -1;
    FreeSpaceMap &fsm = __freeSpace[(string &)fileName];
    for (map<int, set<int> >::iterator it = fsm.begin(); it != fsm.end(); ++it) {
        int curSize = it->first;
        if (curSize >= spaceSize) {
            // find the satisfied free space with minimum size
            set<int> &pageSet = it->second;
            for (set<int>::iterator jt = pageSet.begin(); jt != pageSet.end(); ++jt) {
                page = *jt;
                pageSet.erase(jt);
                break;
            }
            if (pageSet.size() == 0) {
                fsm.erase(it);
                break;
            }
        }
    }

    pageNum = page;
//    cout << "allocateSpace(), pageNum: " << pageNum << ". fileName: " << fileName << endl;
    return SUCCESSFUL;
}

// XXX: not tested
/**
 * Deallocate new space of an existing record.
 *
 * @param fileName
 *          (In the current implementation, fileName is not used as we do not update free space map.0
 * @param fileHandle
 * @param pageNum
 *          (return) the page number of the allocated space.
 * @param slot
 *          (return) the slot number of the allocated space.
 * @return status
 */
RC SpaceManager::deallocateSpace(const string &fileName, FileHandle &fileHandle, int pageNum, int slotNum) {
    RC err;

    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         strcmp(fileName.c_str(), fileHandle.getFileName()) != 0) {
        __trace();
        return ERR_BAD_HANDLE;
    }

    // Validate the input while reading the page
    if (pageNum < 0 || pageNum >= (int) fileHandle.getNumberOfPages()) {
        __trace();
        std::cout << "Page number #" << pageNum << " is invalid: the page size: " << fileHandle.getNumberOfPages() << endl;
        return ERR_RECORD_NOT_FOUND;
    }
    void *page = getPageBuffer();
    if ((err = fileHandle.readPage(pageNum, page)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // Get and validate slot number
    unsigned short slotCount = getSlotCount(page);
    if (slotNum < 0 || slotNum >= slotCount) {
        __trace();
        std::cout << "Slot number #" << slotNum << " is invalid: the page slot count: " << slotCount<< endl;
        return ERR_RECORD_NOT_FOUND;
    }

    // Check whether the current read slot is a tomb stone. If so, we need to trace another page.
    short startPos = getSlotStartPos(page, slotNum);
    short length = getSlotLength(page, slotNum);
    bool tombstone = isTombstoneSlot(startPos, length);

    // Lazily delete record by just nullifying the slot directory without reclaiming the actual space.
    nullifySlot(page, slotNum);
    if ((err = fileHandle.writePage(pageNum, page)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // Deallocate the next slot if the current one is a tomb stone
    if (tombstone) {
        int newPageNum, newSlotNum;
        getNewRecordPos(startPos, length, newPageNum, newSlotNum);
        return deallocateSpace(fileName, fileHandle, newPageNum, newSlotNum); // Do recursion
    } else {
        return SUCCESSFUL;
    }
}

// XXX: not tested
/**
 * Deallocate all spaces in the file.
 */
RC SpaceManager::deallocateAllSpaces(const string &fileName, FileHandle &fileHandle) {
    RC err;

    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         strcmp(fileName.c_str(), fileHandle.getFileName()) != 0) {
        __trace();
        return ERR_BAD_HANDLE;
    }

    clearFreeSpaceMap(fileName);

    int freeSize = PAGE_SIZE - getMetadataSize(1);  // reserve one slot for next update
    unsigned pageSize = fileHandle.getNumberOfPages();
    for (unsigned i = 0; i < pageSize; i++) {
        void *page = getPageBuffer();
        if ((err = fileHandle.readPage(i, page)) != SUCCESSFUL) {
            __trace();
            return err;
        }

        // For each page, directly reset freePtr to 0, slot count to 0.
        setFreePtr(page, 0);  // beginning
        setSlotCount(page, 0);
        if ((err == fileHandle.writePage(i, page)) != SUCCESSFUL) {
            __trace();
            return err;
        }

        // Reset the free page size (reserve one more slot directory space)
        updateFreeSpaceMap(fileName, i, freeSize);
    }

    return SUCCESSFUL;
}

/**
 * Insert the information about the free slot into the buffered
 * free space map in memory.
 *
 * @param fileName
 * @param pageNum
 * @param startPosition
 * @param size
 *          the size of the free space
 */
void SpaceManager::updateFreeSpaceMap(const string &fileName, int pageNum, int size) {
    if (size > 0) {
        __freeSpace[(string &)fileName][size].insert(pageNum);
    }
}

/**
 * Clear the free space map.
 */
void SpaceManager::clearFreeSpaceMap(const string &fileName) {
    __freeSpace.erase((string &)fileName);
}

/**
 * Print the free space map for debugging purposes.
 */
void SpaceManager::printFreeSpaceMap() {
    __trace();
    std::cout << "### Free Space Map ###" << std::endl;
    for (map<string, FreeSpaceMap>::iterator it = __freeSpace.begin(); it != __freeSpace.end(); ++it) {
        std::cout << "File Name: " << it->first << std::endl;
        FreeSpaceMap fsm = it->second;
        for (map<int, set<int> >::iterator jt = fsm.begin(); jt != fsm.end(); ++jt) {
            std::cout << "\tFree Size: " << jt->first << std::endl;
            set<int> pageSet = jt->second;
            for (set<int>::iterator kt = pageSet.begin(); kt != pageSet.end(); ++kt) {
                std::cout << "\t\tPage " << *kt << std::endl;
            }
        }
    }
}

/**
 * Utility functions for page manipulations
 *
 */

unsigned short SpaceManager::getFreePtr(void *page) {
    unsigned short ret;
    int offset = PAGE_SIZE - sizeof(unsigned short);
    memcpy((char *)&ret, (char *)page + offset, sizeof(unsigned short));
    return ret;
}

unsigned short SpaceManager::getSlotCount(void *page) {
    unsigned short ret;
    int offset = PAGE_SIZE - 2 * sizeof(unsigned short);
    memcpy((char *)&ret, (char *)page + offset, sizeof(unsigned short));
    return ret;
}

short SpaceManager::getSlotStartPos(void *page, int slotNum) {
    short ret;
    int offset = PAGE_SIZE - 2 * sizeof(unsigned short) - (slotNum + 1) * sizeof(int);
    memcpy((char *)&ret, (char *)page + offset, sizeof(short));
    return ret;
}

short SpaceManager::getSlotLength(void *page, int slotNum) {
    unsigned short ret;
    int offset = PAGE_SIZE - 2 * sizeof(unsigned short) - (slotNum + 1) * sizeof(int) + sizeof(unsigned short);
    memcpy((char *)&ret, (char *)page + offset, sizeof(unsigned short));
    return ret;
}

void SpaceManager::setFreePtr(void *page, unsigned short data) {
    int offset = PAGE_SIZE - sizeof(unsigned short);
    memcpy((char *)page + offset, (char *)&data, sizeof(unsigned short));
}

void SpaceManager::setSlotCount(void *page, unsigned short data) {
    int offset = PAGE_SIZE - 2 * sizeof(unsigned short);
    memcpy((char *)page + offset, (char *)&data, sizeof(unsigned short));
}

void SpaceManager::setSlotStartPos(void *page, int slotNum, short data) {
    int offset = PAGE_SIZE - 2 * sizeof(unsigned short) - (slotNum + 1) * sizeof(int);
    memcpy((char *)page + offset, (char *)&data, sizeof(short));
}

void SpaceManager::setSlotLength(void *page, int slotNum, short data) {
    int offset = PAGE_SIZE - 2 * sizeof(unsigned short) - (slotNum + 1) * sizeof(int) + sizeof(unsigned short);
    memcpy((char *)page + offset, (char *)&data, sizeof(unsigned short));
}

void SpaceManager::setSlot(void *page, int slotNum, short start, short length) {
    setSlotStartPos(page, slotNum, start);
    setSlotLength(page, slotNum, length);
}

void SpaceManager::writeRecord(void *page, const void *data, unsigned short start, unsigned size) {
    memcpy((char *)page + start, data, size);
}

void SpaceManager::readRecord(const void *page, void *data, unsigned short start, unsigned size) {
    memcpy(data, (const char *)page + start, size);
}

int SpaceManager::getMetadataSize(int slotNum) {
    return 2 * sizeof(unsigned short) + slotNum * sizeof(int);
}

bool SpaceManager::hasFreeExistingSlot(void *page, unsigned short slotCount, unsigned short &firstFreeSlot) {
    bool hasFreeSlot = false;
    for (unsigned short i = 0; i < slotCount; i++) {
        short start = getSlotStartPos(page, i);
        if (start < -1 || start >= PAGE_SIZE - 4 * (1 + slotCount)) {
            return ERR_BAD_DATA;
        }
        if (start == -1) {
            hasFreeSlot = true;
            firstFreeSlot = i;
            break;
        }
    }
    return hasFreeSlot;
}

bool SpaceManager::isTombstoneSlot(short startPos, short size) {
    return (startPos < 0 && size < 0);
}

void SpaceManager::setTombstoneSlot(void *page, int slotNum, short newPageNum, short newSlotNum) {
    setSlot(page, slotNum, -newPageNum, -newSlotNum);
}

void SpaceManager::getNewRecordPos(short startPos, short length, int &newPageNum, int &newSlotNum) {
    newPageNum = -startPos;
    newSlotNum = -length;
}

void SpaceManager::nullifySlot(void *page, int slotNum) {
    setSlot(page, slotNum, PAGE_SIZE, 0);
}

void SpaceManager::initCleanPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    // create a dummy slot for prospect first record
    setFreePtr(page, 0);
    setSlotCount(page, 1);
    setSlotStartPos(page, 0, -1);
    setSlotLength(page, 0, 0);
}
