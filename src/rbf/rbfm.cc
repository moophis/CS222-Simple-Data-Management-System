#include <iostream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <map>
#include <set>
#include <utility>
#include <vector>
#include <algorithm>
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
        __trace();
//        cout << "--> err = " << err << endl;
        return err;
    }
    if ((err = SpaceManager::instance()->bufferSizeInfo(fileName, fileHandle)) != SUCCESSFUL) {
        __trace();
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

    return __insertRecord(fileName, fileHandle, data, rid, recordSize);
}

/**
 * Helper function for insertRecord(). It finds a free space (or appends a new page),
 * then wire the record.
 */
RC RecordBasedFileManager::__insertRecord(const string &fileName,
        FileHandle &fileHandle, const void *data, RID &rid, unsigned recordSize) {
    RC err = 0;

//    __trace();
    // Find and allocate a fit space to store the record: get page # and start position
    // if cannot find an existing page then append and initialize a new page
    int pageNum = -1;
    if ((err = SpaceManager::instance()->allocateSpace(fileName, fileHandle, recordSize, pageNum)) != SUCCESSFUL) {
        return err;
    }

//    void *page = SpaceManager::getPageBuffer();
    char page[PAGE_SIZE];
    if (pageNum == -1) {
//        __trace();
//        cout << "--> record: start = " << 0 << ", size = " << recordSize
//             << " @page " << fileHandle.getNumberOfPages() << " free size " << endl;

        // need to append a new page
        SpaceManager::instance()->initCleanPage(page);
        pageNum = fileHandle.getNumberOfPages();
        SpaceManager::instance()->writeRecord(page, data, 0, recordSize);  // wire record data
        SpaceManager::instance()->setFreePtr(page, recordSize);           // update free pointer
        SpaceManager::instance()->setSlotCount(page, 1);                 // update # of slots
        SpaceManager::instance()->setSlot(page, 0, 0, recordSize);        // set start position of first slot

        // append that page
        if ((err = fileHandle.appendPage(page)) != SUCCESSFUL) {
            __trace();
            return err;
        }

        // update metadata, we need reserve one more slot since we don't have free existing ones
        int freeSize = PAGE_SIZE - recordSize - SpaceManager::instance()->getMetadataSize(2);  // existing + reserved one
        SpaceManager::instance()->insertFreeSpaceMap(fileName, pageNum, freeSize);

        // update RID
        rid.pageNum = (unsigned) pageNum;
        rid.slotNum = 0;
    } else {
//        __trace();
        // use existing page, read that page
        if ((err = fileHandle.readPage(pageNum, page)) != SUCCESSFUL) {
            cout << "pageNum: " << pageNum << endl;
            __trace();
            return err;
        }

        // find place and insert record
        unsigned short start = SpaceManager::instance()->getFreePtr(page);
        SpaceManager::instance()->writeRecord(page, data, start, recordSize);

//        __trace();
//        cout << "--> record: start = " << start << ", size = " << recordSize
//             << " @page " << pageNum << endl;


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
        SpaceManager::instance()->insertFreeSpaceMap(fileName, pageNum, freeSize);

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
        __trace();
        cout << "--> pageNum " << rid.pageNum << " exceeded pageCount " << pageCount << endl;
        return ERR_RECORD_NOT_FOUND;
    }

    fileHandle.readPage(rid.pageNum, page);
    unsigned slotCount = SpaceManager::instance()->getSlotCount(page);
    if (rid.slotNum >= slotCount) {
        __trace();
        cout << "--> slotNum " << rid.slotNum << " exceeded slotCount "
             << slotCount << " @page " << rid.pageNum << endl;
        return ERR_RECORD_NOT_FOUND;
    }

    short startPos = SpaceManager::instance()->getSlotStartPos(page, rid.slotNum);
    short recordLength = SpaceManager::instance()->getSlotLength(page, rid.slotNum);
    // The slot is deleted or just bad formatted (due to file inconsistency)
    if (startPos >= PAGE_SIZE || recordLength >= PAGE_SIZE) {
        __trace();
        return ERR_BAD_DATA;
    }

    // Deal with the case where the slot directory is a tomb stone
    if (SpaceManager::instance()->isTombstoneSlot(startPos, recordLength)) {
        __trace();
        RID newRid;
        SpaceManager::instance()->getNewRecordPos(startPos, recordLength, newRid.pageNum, newRid.slotNum);
        return readRecord(fileHandle, recordDescriptor, newRid, data);
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

// XXX: not tested
/**
 * Given a record descriptor, update the record identified by the given rid with the passed data.
 * If the record grows and there is no space in the page to store the record, the record is migrated
 * to a new page with enough free space.
 */
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         fileHandle.getFileName() == NULL) {
        return ERR_BAD_HANDLE;
    }

    RC err = 0;
    string fileName(fileHandle.getFileName());

    // Validate the input while reading the page
    if (rid.pageNum >= fileHandle.getNumberOfPages()) {
        __trace();
        std::cout << "Page number #" << rid.pageNum << " is invalid: the page size: " << fileHandle.getNumberOfPages() << endl;
        return ERR_RECORD_NOT_FOUND;
    }
//    void *page = SpaceManager::getPageBuffer();
    char page[PAGE_SIZE];  // use stack instead
    if ((err = fileHandle.readPage(rid.pageNum, page)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // Get and validate slot number
    unsigned short slotCount = SpaceManager::instance()->getSlotCount(page);
    if (rid.slotNum >= slotCount) {
        __trace();
        std::cout << "Slot number #" << rid.slotNum << " is invalid: the page slot count: " << slotCount << endl;
        return ERR_RECORD_NOT_FOUND;
    }

    // Calculate the size of the new record
    unsigned recordSize;
    if ((err = countRecordSize(recordDescriptor, data, recordSize)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // Handle the case when the slot is a tomb stone
    short startPos = SpaceManager::instance()->getSlotStartPos(page, rid.slotNum);
    short oldRecordSize = SpaceManager::instance()->getSlotLength(page, rid.slotNum);
    __trace();
    cout << "--> record: start = " << startPos << ", size = " << oldRecordSize
         << " @page " << rid.pageNum << " free size " << SpaceManager::instance()->getPageFreeSize(page) << endl;
    if (SpaceManager::instance()->isTombstoneSlot(startPos, oldRecordSize)) {
        unsigned newPageNum, newSlotNum;
        SpaceManager::instance()->getNewRecordPos(startPos, oldRecordSize, newPageNum, newSlotNum);
        // Recursively find the real place of the record
        RID nrid;
        nrid.pageNum = newPageNum;
        nrid.slotNum = newSlotNum;
        __trace();
        cout << "--> Find a tombstone, new place @page " << nrid.pageNum << " slot " << nrid.slotNum << endl;
        return updateRecord(fileHandle, recordDescriptor, data, nrid);
    } else {
        // Find out if the new record can fit in the current page
        unsigned freeSize = SpaceManager::instance()->getPageFreeSize(page);

        if (recordSize <= (unsigned) oldRecordSize) {
            __trace();
            // Update record in the old place
            SpaceManager::instance()->writeRecord(page, data,
                    (unsigned short) startPos, (unsigned short) recordSize);
            // Update slot directory
            SpaceManager::instance()->setSlot(page, rid.slotNum, startPos, recordSize);
        } else if (recordSize < freeSize) {
            __trace();
            // Find and update slot in place and free pointer
            unsigned short freePtr = SpaceManager::instance()->getFreePtr(page);
            SpaceManager::instance()->writeRecord(page, data, freePtr, recordSize);
            SpaceManager::instance()->setSlot(page, rid.slotNum, freePtr, recordSize);
            SpaceManager::instance()->setFreePtr(page, freePtr + recordSize);
            // Update free space map
            if ((err = SpaceManager::instance()->updateFreeSpaceMap(fileName, rid.pageNum,
                        freeSize, freeSize - recordSize)) != SUCCESSFUL) {
                __trace();
                return err;
            }
        } else {
            __trace();
            // Find another page to store the record and leave a tomb stone in the old place
            RID newRid;
            if ((err = __insertRecord(fileName, fileHandle, data, newRid, recordSize)) != SUCCESSFUL) {
                __trace();
                return err;
            }
            SpaceManager::instance()->setTombstoneSlot(page, rid.slotNum, (short) newRid.pageNum, (short) newRid.slotNum);
        }

        // write back the page
        if ((err = fileHandle.writePage(rid.pageNum, page)) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }

    return SUCCESSFUL;
}

// XXX: not tested
/**
 * Given a record descriptor, read a specific attribute of a record identified by a given rid.
 */
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid,
        const string attributeName, void *data) {
    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         fileHandle.getFileName() == NULL) {
        return ERR_BAD_HANDLE;
    }

    RC err;

    // Read that page
    void *page = SpaceManager::getPageBuffer();
    if ((err = fileHandle.readPage(rid.pageNum, page)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // Read slot information
    unsigned short slotCount = SpaceManager::instance()->getSlotCount(page);
    if (rid.slotNum >= slotCount) {
        __trace();
        std::cout << "Slot number #" << rid.slotNum << " is invalid: the page slot count: " << slotCount << endl;
        return ERR_RECORD_NOT_FOUND;
    }
    short startPos = SpaceManager::instance()->getSlotStartPos(page, rid.slotNum);
    short recordSize = SpaceManager::instance()->getSlotLength(page, rid.slotNum);

    // Check if the current slot is a tomb stone
    if (SpaceManager::instance()->isTombstoneSlot(startPos, recordSize)) {
        unsigned newPageNum, newSlotNum;
        SpaceManager::instance()->getNewRecordPos(startPos, recordSize, newPageNum, newSlotNum);
        // Recursively find the real place of the record
        RID nrid;
        nrid.pageNum = newPageNum;
        nrid.slotNum = newSlotNum;
        return readAttribute(fileHandle, recordDescriptor, nrid, attributeName, data);
    } else {
        unsigned dataSize;
        return __readAttribute(page, startPos, recordDescriptor, attributeName, data, dataSize);
    }
}

RC RecordBasedFileManager::__readAttribute(void *page, unsigned short startPos, const vector<Attribute> &recordDescriptor,
            const string attributeName, void *data, unsigned &dataSize) {
    unsigned offset = startPos;

    for (size_t i = 0; i < recordDescriptor.size(); i++) {
        Attribute attr = recordDescriptor[i];
        string name = attr.name;
        AttrType type = attr.type;
        AttrLength length = attr.length;

        switch (type) {
        case TypeVarChar:{
            AttrLength len = 0;
            memcpy((char *)&len, (char *)page + offset, sizeof(int));
            if (len > length) {
                __trace();
                cout << "readAttribute(): string len = " << len << ", should be " << length << endl;
                return ERR_FORMAT;
            }

            // Returned data includes: 4 byte of chars size + chars
            if (name.compare(attributeName) == 0) {
                memcpy((char *)data, (char *)page + offset, len + sizeof(int));
                dataSize = len + sizeof(int);
                return SUCCESSFUL;
            }

            offset += sizeof(int);  // length itself
            offset += len;          // string length
            break;
        }
        case TypeInt:
            if (name.compare(attributeName) == 0) {
                memcpy((char *)data, (char *)page + offset, sizeof(int));
                dataSize = sizeof(int);
                return SUCCESSFUL;
            }
            offset += sizeof(int);
            break;
        case TypeReal:
            if (name.compare(attributeName) == 0) {
                memcpy((char *)data, (char *)page + offset, sizeof(float));
                dataSize = sizeof(float);
                return SUCCESSFUL;
            }
            offset += sizeof(float);
            break;
        default:
            return ERR_UNKNOWN_TYPE;
        }
    }

    return ERR_ATTR_NOT_FOUND;
}

// XXX: not tested
/**
 * Given a record descriptor, reorganize a page, i.e., push the free space towards the end of the page.
 */
RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber) {
    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         fileHandle.getFileName() == NULL) {
        return ERR_BAD_HANDLE;
    }

    RC err;

    // Validate page number
    if (pageNumber >= fileHandle.getNumberOfPages()) {
        __trace();
        std::cout << "Page number #" << pageNumber << " is invalid: the page size: " << fileHandle.getNumberOfPages() << endl;
        return ERR_RECORD_NOT_FOUND;
    }
//    void *page = SpaceManager::getPageBuffer();
    char page[PAGE_SIZE];
    if ((err = fileHandle.readPage(pageNumber, page)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // Iterate through the slot directory and buffer the slot information
    vector<pair<short, unsigned> > records;  // {<startPos, slot #>}
    unsigned short slotCount = SpaceManager::instance()->getSlotCount(page);
    for (unsigned i = 0; i < slotCount; i++) {
        short startPos = SpaceManager::instance()->getSlotStartPos(page, i);
        short len = SpaceManager::instance()->getSlotLength(page, i);
        if (SpaceManager::instance()->isOccupiedSlot(startPos, len)) {
            records.push_back(make_pair(startPos, i));
        }
    }

    // Sort the buffer based on startPos and rearrange the record based on that
    std::sort(records.begin(), records.end());
    unsigned offset = 0;
    for (auto& r : records) {
        short startPos = r.first;
        unsigned slotNum = r.second;
        short len = SpaceManager::instance()->getSlotLength(page, slotNum);
        if (len < 0) {
            cout << "The len should not be " << len << endl;
            return ERR_BAD_DATA;
        }

        // Set new slot start position
        SpaceManager::instance()->setSlotStartPos(page, slotNum, offset);
        // Move
        memmove((char *)page + offset, (char *)page + startPos, (size_t) len);
        offset += len;
    }

    // Reset free pointer
    SpaceManager::instance()->setFreePtr(page, offset);

    // Write back
    if ((err = fileHandle.writePage(pageNumber, page)) != SUCCESSFUL) {
        return err;
    }

    return SUCCESSFUL;
}

// XXX: not tested
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

    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         fileHandle.getFileName() == NULL) {
        return ERR_BAD_HANDLE;
    }
    if (compOp != NO_OP && (value == NULL || conditionAttribute.empty())) {
        return ERR_INV_COND;
    }

    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = (void *) value;
    rbfm_ScanIterator.attributeNames = attributeNames;
    rbfm_ScanIterator.fileHandle = fileHandle;

    rbfm_ScanIterator.nextPageNum = 0;
    rbfm_ScanIterator.nextSlotNum = 0;
    rbfm_ScanIterator.active = true;

    return SUCCESSFUL;
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
 * RBFM_ScanIterator Implementations.
 * XXX: not tested
 */
RBFM_ScanIterator::RBFM_ScanIterator() {
    this->active = false;
}

RBFM_ScanIterator::~RBFM_ScanIterator() {}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    if (!this->active) {
        return RBFM_EOF;
    }

    unsigned pageCount = fileHandle.getNumberOfPages();
    void *page = SpaceManager::getPageBuffer();
    unsigned slotCount;

    // Scan slots onward until finding the first record meeting the criterion
    short startPos = -1;
    short recordLen = -1;
    bool foundNext = false;
    while (nextPageNum < pageCount) {
        if (fileHandle.readPage(nextPageNum, page) != SUCCESSFUL) {
            __trace();
            return RBFM_EOF;
        }

        slotCount = SpaceManager::instance()->getSlotCount(page);
        while (nextSlotNum < slotCount) {
            short s = SpaceManager::instance()->getSlotStartPos(page, nextSlotNum);
            short len = SpaceManager::instance()->getSlotLength(page, nextSlotNum);
            if (SpaceManager::instance()->isOccupiedSlot(s, len)) {
                // Check condition
                if (meetCriterion(page, (unsigned) s, (unsigned) len)) {
                    foundNext = true;
                    startPos = s;
                    recordLen = len;
                    break;
                }
            }
            nextSlotNum++;
        }

        if (foundNext) {
            break;
        } else {  // Get next page
            nextPageNum++;
            nextSlotNum = 0;
        }
    }
    if (!foundNext) {
        return RBFM_EOF;
    }

    // Read data and assemble results
    unsigned offset = 0;
    for (size_t i = 0; i < attributeNames.size(); i++) {
        char attData[recordLen];
        char *attPtr = attData;
        unsigned dataSize;
        if (RecordBasedFileManager::instance()->__readAttribute(page, startPos,
                this->recordDescriptor, attributeNames[i], &attData, dataSize) != SUCCESSFUL) {
            __trace();
            return RBFM_EOF;
        }
        memcpy((char *)data + offset, attPtr, dataSize);
        offset += dataSize;
    }

    // Update rid and next slot to visit
    rid.pageNum = nextPageNum;
    rid.slotNum = nextSlotNum++;
    return SUCCESSFUL;
}

RC RBFM_ScanIterator::close() {
    this->active = false;
    return SUCCESSFUL;
}

bool RBFM_ScanIterator::meetCriterion(void *page, unsigned short startPos, unsigned short length) {
    if (compOp == NO_OP) {
        return true;
    }

    // Get attribute to be compared
    bool found = false;
    Attribute attr;
    for (size_t i = 0; i < recordDescriptor.size(); i++) {
        attr = recordDescriptor[i];
        if (conditionAttribute.compare(attr.name) == 0) {
            found = true;
            break;
        }
    }
    if (!found) {
        return false;
    }

    // Read attribute
    unsigned dataSize;
    char data[length];
    if (RecordBasedFileManager::instance()->__readAttribute(page, startPos,
            this->recordDescriptor, conditionAttribute, &data, dataSize) != SUCCESSFUL) {
        return false;
    }

    // Compare
    bool matched = false;
    switch (attr.type) {
    case TypeVarChar: {
        AttrLength len = 0;
        memcpy((char *)&len, (char *)&data, sizeof(int));
        char str[len+1];
        char *s = str;
        memcpy(s, (char *)&data + sizeof(int), len);
        str[len] = 0;
        // Here we assume that if value is a string, it's null terminated.
        matched = evaluateString(string(str), this->compOp, string((char *)this->value));
        break;
    }
    case TypeInt:
        int integer;
        memcpy((char *)&integer, (char *)&data, sizeof(int));
        matched = evaluateNumber<int>(integer, this->compOp, *((int *)this->value));
        break;
    case TypeReal:
        float real;
        memcpy((char *)&real, (char *)&data, sizeof(float));
        matched = evaluateNumber<float>(real, this->compOp, *((float *)this->value));
        break;
    default:
        break;
    }

    return matched;
}

template <class T>
bool RBFM_ScanIterator::evaluateNumber(T number, CompOp compOP, T val) {
    switch (compOp) {
    case EQ_OP:
        return number == val;
    case LT_OP:
        return number < val;
    case GT_OP:
        return number > val;
    case LE_OP:
        return number <= val;
    case GE_OP:
        return number >= val;
    case NE_OP:
        return number != val;
    case NO_OP:
    default:
        return false;
    }

    return false;
}

bool RBFM_ScanIterator::evaluateString(string str, CompOp compOp, string val) {
    switch (compOp) {
    case EQ_OP:
        return str.compare(val) == 0;
    case LT_OP:
        return str.compare(val) < 0;
    case GT_OP:
        return str.compare(val) > 0;
    case LE_OP:
        return str.compare(val) <= 0;
    case GE_OP:
        return str.compare(val) >= 0;
    case NE_OP:
        return str.compare(val) != 0;
    case NO_OP:
    default:
        return false;
    }

    return false;
}


/**
 * Space Manager Implementations
 */
typedef map<int, set<int> > FreeSpaceMap;    // <free space size, corresponding locations (page #)>

//map<string, FreeSpaceMap> SpaceManager::__freeSpace;

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
 * TODO: deprecate that function!!
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

        unsigned short firstSlot = PAGE_SIZE;
        if (!hasFreeExistingSlot(buffer, slotCount, firstSlot)) {
            slotCount++;
        } else {
            __trace();
            cout << "First free slot @page " << i << " , slot " << firstSlot
                 << " pageCount: " << pageNum << ", slotCount: " << slotCount
                 << " free Ptr: " << freePtr << endl;
        }

        int freeSize = PAGE_SIZE - freePtr - getMetadataSize(slotCount);
//        cout << "freeSize " << freeSize << endl;
        insertFreeSpaceMap(fileName, i, freeSize);
    }

//    __trace();
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

//    __trace();
    /*
     * Retrieve suitable free page according to metadata
     * Note: the free space map will also delete the map entry of allocated space
     * and clean zero length entry if possible.
     */
    int page = -1;
//    printFreeSpaceMap();
    FreeSpaceMap &fsm = __freeSpace[(string &)fileName];
//    __trace();
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

//    __trace();
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
RC SpaceManager::deallocateSpace(const string &fileName, FileHandle &fileHandle, unsigned pageNum, unsigned slotNum) {
    RC err;

    if (!fileHandle.getFilePointer() ||
         fileHandle.getNumberOfPages() < 0 ||
         strcmp(fileName.c_str(), fileHandle.getFileName()) != 0) {
        __trace();
        return ERR_BAD_HANDLE;
    }

    // Validate the input while reading the page
    if (pageNum >= fileHandle.getNumberOfPages()) {
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
    if (slotNum >= slotCount) {
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
        unsigned newPageNum, newSlotNum;
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
        insertFreeSpaceMap(fileName, i, freeSize);
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
void SpaceManager::insertFreeSpaceMap(const string &fileName, int pageNum, int size) {
    if (size > 0) {
        __freeSpace[(string &)fileName][size].insert(pageNum);
    }
}

RC SpaceManager::updateFreeSpaceMap(const string &fileName, int pageNum, int oldSize, int newSize) {
    if (oldSize > 0 && newSize > 0) {
        if (__freeSpace[(string &)fileName][oldSize].count(pageNum) > 0) {
            __freeSpace[(string &)fileName][oldSize].erase(pageNum);
            if (__freeSpace[(string &)fileName][oldSize].size() == 0) {
                __freeSpace[(string &)fileName].erase(oldSize);
            }
            __freeSpace[(string &)fileName][newSize].insert(pageNum);
            return SUCCESSFUL;
        } else {
            cout << "The free size of the page " << pageNum << " should not be " << oldSize << endl
                 << "There must be something wrong with the free page calculation!" << endl;
            return ERR_MAP_ENTRY_NOT_FOUND;
        }
    }

    return SUCCESSFUL;
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

short SpaceManager::getSlotStartPos(void *page, unsigned slotNum) {
    short ret;
    int offset = PAGE_SIZE - 2 * sizeof(unsigned short) - (slotNum + 1) * sizeof(int);
    memcpy((char *)&ret, (char *)page + offset, sizeof(short));
    return ret;
}

short SpaceManager::getSlotLength(void *page, unsigned slotNum) {
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

void SpaceManager::setSlotStartPos(void *page, unsigned slotNum, short data) {
    int offset = PAGE_SIZE - 2 * sizeof(unsigned short) - (slotNum + 1) * sizeof(int);
    memcpy((char *)page + offset, (char *)&data, sizeof(short));
}

void SpaceManager::setSlotLength(void *page, unsigned slotNum, short data) {
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

unsigned SpaceManager::getMetadataSize(int slotCount) {
    return 2 * sizeof(unsigned short) + slotCount * sizeof(int);
}

// Note the free space calculation policy should be identical with
// the one in insertRecord().
unsigned SpaceManager::getPageFreeSize(void *page) {
    unsigned short firstFree;
    unsigned short slotCount = getSlotCount(page);
    if (!hasFreeExistingSlot(page, slotCount, firstFree)) {
        slotCount++;
    }
    unsigned short freePtr = getFreePtr(page);
    if ((int)(PAGE_SIZE - freePtr - getMetadataSize(slotCount)) < 0) {
        cout << "!!!!Negative free space: " << (int)(PAGE_SIZE - freePtr - getMetadataSize(slotCount)) << endl;
    }
    return PAGE_SIZE - freePtr - getMetadataSize(slotCount);
}

bool SpaceManager::hasFreeExistingSlot(void *page, unsigned short slotCount, unsigned short &firstFreeSlot) {
    bool hasFreeSlot = false;
//    __trace();
//    cout << "--slotCount " << slotCount << endl;
    for (unsigned short i = 0; i < slotCount; i++) {
        short start = getSlotStartPos(page, i);
        short len = getSlotLength(page, i);
//        cout << "---- i = " << i << " , startPos = " << start
//             << " length = " << len << endl;

        if (isDeletedSlot(start, len)) {
            hasFreeSlot = true;
            firstFreeSlot = i;
//            cout << "\t---Find free slot: " << firstFreeSlot << endl;
            break;
        }
    }
    return hasFreeSlot;
}

bool SpaceManager::isTombstoneSlot(short startPos, short size) {
    return (startPos < 0 && size < 0) || (startPos == 0 && size == 0);
}

bool SpaceManager::isOccupiedSlot(short startPos, short length) {
    return (startPos >= 0 && startPos < PAGE_SIZE && length > 0);
}

bool SpaceManager::isDeletedSlot(short startPos, short length) {
    return (startPos == PAGE_SIZE && length == 0);
}

void SpaceManager::setTombstoneSlot(void *page, int slotNum, short newPageNum, short newSlotNum) {
    setSlot(page, slotNum, -newPageNum, -newSlotNum);
}

void SpaceManager::getNewRecordPos(short startPos, short length, unsigned &newPageNum, unsigned &newSlotNum) {
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
//    setSlotStartPos(page, 0, -1);
//    setSlotLength(page, 0, 0);
    nullifySlot(page, 0);
}
