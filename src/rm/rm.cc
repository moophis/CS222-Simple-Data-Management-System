
#include "rm.h"

// Constant definitions
#define TABLES_NAME             "Tables"
#define COLUMNS_NAME            "Columns"
#define TABLE_FILE_SUFFIX       ".tbl"
const int TABLES_ID             = 0;
const int COLUMNS_ID            = 1;
const int MAX_NAME_LEN          = 300;

RelationManager* RelationManager::_rm = 0;

RecordBasedFileManager* RelationManager::_rbfm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    RC err;
    _rbfm = RecordBasedFileManager::instance();

    getAdmin();

    initCatalogSchema();

    FileHandle tablesHandle;
    FileHandle columnsHandle;
    bool newTablesFile = false, newColumnsFile = false;
    err = initCatalogFile(TABLES_NAME, tablesHandle, newTablesFile);
    assert(err == SUCCESSFUL);
    err = initCatalogFile(COLUMNS_NAME, columnsHandle, newColumnsFile);
    assert(err == SUCCESSFUL);

    // Put file handle info into the handle map
    cacheTableHandle(TABLES_NAME, tablesHandle);
    cacheTableHandle(COLUMNS_NAME, columnsHandle);
//    printTableHandleMap();
    maxTableId = 1;  // 0 and 1 has been reserved

    // Wire metadata of those two files in the catalog
    if (newTablesFile || newColumnsFile) {
        __trace();
        err = wireTableMetadata(TABLES_NAME, TABLES_ID, tablesSchema);
        assert(err == SUCCESSFUL);
        err = wireTableMetadata(COLUMNS_NAME, COLUMNS_ID, columnsSchema);
        assert(err == SUCCESSFUL);
    }
//    printCatalogMaps();

    // Scan the ".tables" and ".columns" file to build other maps
    // and find the maxTableId.
    err = bufferMappings();
    assert(err == SUCCESSFUL);
//    __trace();
//    printCatalogMaps();
//    cout << "maxTableId: " << maxTableId << endl;

    yieldAdmin();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC err;
    if (!isPrivileged(tableName)) {
        return ERR_NO_PERMISSION;
    }

    // Create new file for the table
    if ((err = _rbfm->createFile(getFileName(tableName))) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Register table mapping
    int tableId = ++maxTableId;

    // Wire schema info into catalog files
    if ((err = wireTableMetadata(tableName, tableId, attrs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RC err;
    if (!isPrivileged(tableName)) {
        return ERR_NO_PERMISSION;
    }

    // Close file (if opened) and drop the cached file handle
    FileHandle handle;
    if ((err = getCachedTableHandle(tableName, handle)) == SUCCESSFUL) {
        _rbfm->closeFile(handle);
        dropTableHandle(tableName);
    }

    // Delete the file of the table
    if ((err = _rbfm->destroyFile(getFileName(tableName))) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Delete catalog metadata (including map entries in memory)
    if ((err = dropTableMetadata(tableName)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RC err;

    int tableId;
    if ((err = getTableId(tableName, tableId)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    vector<pair<Attribute, RID> > attrPairs;
    if ((err = getTableAttributes(tableId, attrPairs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }
    for (unsigned i = 0; i < attrPairs.size(); i++) {
        attrs.push_back(attrPairs[i].first);
    }
    return SUCCESSFUL;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
//    __trace();
    RC err;

    if (!isPrivileged(tableName)) {
        return ERR_NO_PERMISSION;
    }

    // Get file handle and update handle cache
    FileHandle fileHandle;
    if ((err = getTableFileHandle(tableName, fileHandle)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }
    cacheTableHandle(tableName, fileHandle);

    // Get table attribute descriptor
    vector<Attribute> attrs;
    if ((err = getAttributes(tableName, attrs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Call RBFM layer
    if ((err = _rbfm->insertRecord(fileHandle, attrs, data, rid)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::deleteTuples(const string &tableName)
{
    __trace();
    RC err;

    if (!isPrivileged(tableName)) {
        return ERR_NO_PERMISSION;
    }

    // Get file handle
    FileHandle fileHandle;
    if ((err = getTableFileHandle(tableName, fileHandle)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }
    cacheTableHandle(tableName, fileHandle);

    // Call RBFM layer
    if ((err = _rbfm->deleteRecords(fileHandle)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    __trace();
    RC err;

    if (!isPrivileged(tableName)) {
        return ERR_NO_PERMISSION;
    }

    // Get file handle
    FileHandle fileHandle;
    if ((err = getTableFileHandle(tableName, fileHandle)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }
    cacheTableHandle(tableName, fileHandle);

    // Get table attribute descriptor
    vector<Attribute> attrs;
    if ((err = getAttributes(tableName, attrs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Call RBFM layer
    if ((err = _rbfm->deleteRecord(fileHandle, attrs, rid)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
//    __trace();
    RC err;

    if (!isPrivileged(tableName)) {
        return ERR_NO_PERMISSION;
    }

    // Get file handle
    FileHandle fileHandle;
    if ((err = getTableFileHandle(tableName, fileHandle)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }
    cacheTableHandle(tableName, fileHandle);

    // Get table attribute descriptor
    vector<Attribute> attrs;
    if ((err = getAttributes(tableName, attrs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Call RBFM layer
    if ((err = _rbfm->updateRecord(fileHandle, attrs, data, rid)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
//    __trace();
    RC err;

    // Get file handle
    FileHandle fileHandle;
    if ((err = getTableFileHandle(tableName, fileHandle)) != SUCCESSFUL) {
        __trace();
//        cout << "err = " << err << endl;
        return err;
    }
    cacheTableHandle(tableName, fileHandle);

    // Get table attribute descriptor
    vector<Attribute> attrs;
    if ((err = getAttributes(tableName, attrs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Call RBFM layer
    if ((err = _rbfm->readRecord(fileHandle, attrs, rid, data)) != SUCCESSFUL) {
        __trace();
//        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RC err;

    // Get file handle
    FileHandle fileHandle;
    if ((err = getTableFileHandle(tableName, fileHandle)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    cacheTableHandle(tableName, fileHandle);

    // Get table attribute descriptor
    vector<Attribute> attrs;
    if ((err = getAttributes(tableName, attrs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Call RBFM layer
    if ((err = _rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
//    __trace();
    RC err;

    if (!isPrivileged(tableName)) {
        return ERR_NO_PERMISSION;
    }

    // Get file handle
    FileHandle fileHandle;
    if ((err = getTableFileHandle(tableName, fileHandle)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }
    cacheTableHandle(tableName, fileHandle);

    // Get table attribute descriptor
    vector<Attribute> attrs;
    if ((err = getAttributes(tableName, attrs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Call RBFM layer
    if ((err = _rbfm->reorganizePage(fileHandle, attrs, pageNumber)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    return SUCCESSFUL;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    RC err;

    // Get file handle
    FileHandle fileHandle;
    if ((err = getTableFileHandle(tableName, fileHandle)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }
    cacheTableHandle(tableName, fileHandle);

    // Get table attribute descriptor
    // Note that for catalog files we have to provide attribute info a prior!!!
    vector<Attribute> attrs;
    if (tableName.compare(TABLES_NAME) == 0) {
//        __trace();
//        cout << "Scan " << TABLES_NAME << endl;
        attrs = tablesSchema;
    } else if (tableName.compare(COLUMNS_NAME) == 0) {
//        __trace();
//        cout << "Scan " << COLUMNS_NAME << endl;
        attrs = columnsSchema;
    } else if ((err = getAttributes(tableName, attrs)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }

    // Call RBFM layer
    RBFM_ScanIterator rbfm_ScanIterator;
    if ((err = _rbfm->scan(fileHandle, attrs,
                           conditionAttribute,
                           compOp, value,
                           attributeNames,
                           rbfm_ScanIterator)) != SUCCESSFUL) {
        __trace();
        cout << "err = " << err << endl;
        return err;
    }
    rm_ScanIterator.init(rbfm_ScanIterator);

    return SUCCESSFUL;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    return -1;
}

/**
 * Private functions.
 */
RC RelationManager::initCatalogFile(const string &tableName, FileHandle &handle, bool &newFile) {
    RC err;
    newFile = false;
    while ((err = _rbfm->openFile(tableName, handle)) != SUCCESSFUL) {
        if (err == ERR_NOT_EXIST) {
            newFile = true;
            if ((err == _rbfm->createFile(tableName)) != SUCCESSFUL) {
                __trace();
                cout << "Cannot create table file for " << tableName << endl;
                return err;
            }
        } else {
            __trace();
            cout << "Error no: " << err << endl;
            return err;
        }
    }
    return SUCCESSFUL;
}

void RelationManager::registerTableMapping(const string &tableName, const int tableId, const RID &rid) {
    vector<pair<Attribute, RID> > emptySchema;
    tableNameMap[(string &)tableName] = make_pair(tableId, rid);
    schemaMap[tableId] = emptySchema;
}

// Return the RID of the mapping to be deleted in maps
void RelationManager::deregisterTableMapping(const string &tableName, RID &rid) {
    pair<int, RID> p = tableNameMap[(string &)tableName];
    int id = p.first;
    rid = p.second;
    tableNameMap.erase((string &)tableName);
    schemaMap.erase(id);
}

void RelationManager::appendAttributeMapping(const int tableId, const Attribute &attribute, const RID &rid) {
    schemaMap[tableId].push_back(make_pair(attribute, rid));
}

// Return a list of column RIDs to be deleted
void RelationManager::dropAttributesMapping(const int tableId, vector<RID> &rids) {
    if (schemaMap.count(tableId) > 0) {
        vector<pair<Attribute, RID> > tmp = schemaMap[tableId];
        for (size_t i = 0; i < tmp.size(); i++) {
            rids.push_back(tmp[i].second);
        }

        schemaMap.erase(tableId);
    }
}

void RelationManager::printCatalogMaps() {
    cout << "----- 1. tableNameMap -----" << endl;
    for (auto it = tableNameMap.begin(); it != tableNameMap.end(); ++it) {
        cout << "TableName : " << it->first << " Table Id: " << (it->second).first
             << " @ Page " << (it->second).second.pageNum
             << " Slot " << (it->second).second.slotNum << endl;
    }

    cout << "----- 2. schemaMap -----" << endl;
    for (auto it = schemaMap.begin(); it != schemaMap.end(); ++it) {
        cout << "Table Id: " << it->first << endl;
        for (auto jt = (it->second).begin(); jt != (it->second).end(); ++jt) {
            cout << "\t AttrName: " << (jt->first).name
                 << " Type: " << (jt->first).type
                 << " Length: " << (jt->first).length
                 << " @ Page " << (jt->second).pageNum
                 << " Slot " << (jt->second).slotNum << endl;
        }
    }
}

RC RelationManager::getTableId(const string &tableName, int &tableId) {
    if (tableNameMap.count(tableName) == 0) {
        return ERR_NO_MAP_ENTRY;
    }
    tableId = tableNameMap[(string &) tableName].first;
    return SUCCESSFUL;
}

RC RelationManager::getTableMetadataRID(const string &tableName, RID &rid) {
    if (tableNameMap.count(tableName) == 0) {
        return ERR_NO_MAP_ENTRY;
    }
    rid = tableNameMap[(string &) tableName].second;
    return SUCCESSFUL;
}

RC RelationManager::getTableAttributes(const int tableId, vector<pair<Attribute, RID> > &attrPairs) {
    if (schemaMap.count(tableId) == 0) {
        return ERR_NO_MAP_ENTRY;
    }
    attrPairs = schemaMap[tableId];
    return SUCCESSFUL;
}

void RelationManager::cacheTableHandle(const string &tableName, FileHandle &handle) {
    tableHandles.emplace(tableName, handle);
//    printTableHandleMap();
}

void RelationManager::dropTableHandle(const string &tableName) {
    if (tableHandles.count((string &)tableName) > 0) {
        tableHandles.erase((string &)tableName);
    }
}

RC RelationManager::getCachedTableHandle(const string &tableName, FileHandle &handle) {
    if (tableHandles.count((string &)tableName) == 0) {
        return ERR_HANDLE_NOT_CACHED;
    }
    handle = tableHandles[(string &)tableName];
    return SUCCESSFUL;
}

void RelationManager::printTableHandleMap() {
    cout << "--- File Handles ---" << endl;
    for (auto it = tableHandles.begin(); it != tableHandles.end(); ++it) {
        cout << it->first;
        printFileHandle(it->second);
    }
}

void RelationManager::printFileHandle(FileHandle &handle) {
    cout << " -> name: " << handle.getFileName()
         << ", page count: " << handle.getNumberOfPages()
         << ", file pointer: " << handle.getFilePointer() << endl;
}

RC RelationManager::wireTableMetadata(const string &tableName, const int tableId, const vector<Attribute> &recordAttributes) {
    RC err;
    char buffer[PAGE_SIZE];
    char *bufptr = buffer;
    RID rid;

    // table metadata in ".tables"
    string fileName = tableName + TABLE_FILE_SUFFIX;
    prepareTableRecord(bufptr, tableId, tableName, fileName);
//    __trace();
    if ((err = _rbfm->insertRecord(tableHandles[TABLES_NAME], this->tablesSchema, bufptr, rid))) {
        __trace();
        return err;
    }
//    __trace();
    registerTableMapping(tableName, tableId, rid);

    // table columns metadata in ".columns"
    for (size_t i = 0; i < recordAttributes.size(); i++) {
        Attribute attr = recordAttributes[i];
        prepareColumnRecord(bufptr, tableId, attr.type, attr.length, attr.name);
        if ((err = _rbfm->insertRecord(tableHandles[COLUMNS_NAME], this->columnsSchema, bufptr, rid))) {
            __trace();
            return err;
        }
        appendAttributeMapping(tableId, attr, rid);
    }

//    __trace();
    return SUCCESSFUL;
}

RC RelationManager::dropTableMetadata(const string &tableName) {
    RC err;
    RID rid;
    int tableId;

    // table metadata in ".tables"
    if ((err = getTableId(tableName, tableId)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    if ((err = getTableMetadataRID(tableName, rid)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    deregisterTableMapping(tableName, rid);
    if ((err = _rbfm->deleteRecord(tableHandles[TABLES_NAME], this->tablesSchema, rid)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    // table columns metadata in ".columns"
    vector<RID> rids;
    dropAttributesMapping(tableId, rids);
    for (size_t i = 0; i < rids.size(); i++) {
        if ((err = _rbfm->deleteRecord(tableHandles[COLUMNS_NAME], this->columnsSchema, rids[i])) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }

    return SUCCESSFUL;
}

RC RelationManager::bufferMappings() {
    RC err;

    // First scan ".tables"
    RM_ScanIterator rm_ScanIterator;
    vector<string> tablesAttributes = {"TableID", "TableName", "FileName"}; // TODO: add new attr
    if ((err = scan(TABLES_NAME, "", NO_OP, NULL,
            tablesAttributes, rm_ScanIterator)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    char data[PAGE_SIZE];
    RID rid;
    while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF) {
        unsigned offset = 0;
        // read TableID
        int tableId;
        memcpy((char *)&tableId, data + offset, sizeof(int));
        offset += sizeof(int);
        maxTableId = max(maxTableId, tableId); // find maxTableId
        // read TableName
        int size;
        memcpy((char *)&size, data + offset, sizeof(int));
        offset += sizeof(int);
        char tableName[size+1];
        memcpy(tableName, data + offset, size);
        offset += size;
        tableName[size] = 0;
        // read FileName (TODO)
        int fsize;
        memcpy((char *)&fsize, data + offset, sizeof(int));
        offset += sizeof(int);
        char fileName[fsize+1];
        memcpy(fileName, data + offset, fsize);
        offset += fsize;
        fileName[fsize] = 0;

        // Update table mapping
        registerTableMapping(string(tableName), tableId, rid);
    }
    rm_ScanIterator.close();

    // Then scan ".columns"
    vector<string> columnsAttributes = {"TableID", "ColumnType", "ColumnSize", "AttributeName"};
    if ((err = scan(COLUMNS_NAME, "", NO_OP, NULL,
            columnsAttributes, rm_ScanIterator)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF) {
        unsigned offset = 0;
        // read TableID
        int tableId;
        memcpy((char *)&tableId, data + offset, sizeof(int));
        offset += sizeof(int);
        // read ColumnType
        AttrType type;
        memcpy((char *)&type, data + offset, sizeof(int));
        offset += sizeof(int);
        // read ColumnSize
        unsigned maxSize;
        memcpy((char *)&maxSize, data + offset, sizeof(unsigned));
        offset += sizeof(int);
        // read AttributeName
        int size;
        memcpy((char *)&size, data + offset, sizeof(int));
        offset += sizeof(int);
        char attrName[size+1];
        memcpy(attrName, data + offset, size);
        attrName[size] = 0;

        Attribute attr;
        attr.name = string(attrName);
        attr.type = type;
        attr.length = maxSize;

        // Update attribute mapping
        appendAttributeMapping(tableId, attr, rid);
    }

    return SUCCESSFUL;
}

void RelationManager::initCatalogSchema() {
    // .tables
    Attribute attr;
    attr.name = "TableID";
    attr.type = TypeInt;
    attr.length = sizeof(int);
    tablesSchema.push_back(attr);

    attr.name = "TableName";
    attr.type = TypeVarChar;
    attr.length = MAX_NAME_LEN;  // maximum table name length
    tablesSchema.push_back(attr);

    attr.name = "FileName";      // TODO: new added
    attr.type = TypeVarChar;
    attr.length = MAX_NAME_LEN;  // maximum table name length
    tablesSchema.push_back(attr);

    // .columns
    attr.name = "TableID";
    attr.type = TypeInt;
    attr.length = sizeof(int);
    columnsSchema.push_back(attr);

    attr.name = "ColumnType";  // string, int, real
    attr.type = TypeInt;
    attr.length = sizeof(int);
    columnsSchema.push_back(attr);

    attr.name = "ColumnSize";
    attr.type = TypeInt;
    attr.length = sizeof(int);
    columnsSchema.push_back(attr);

    attr.name = "AttributeName";
    attr.type = TypeVarChar;
    attr.length = MAX_NAME_LEN;  // maximum attribute name length
    columnsSchema.push_back(attr);
}

void RelationManager::prepareTableRecord(char *data, int tableId, string tableName, string fileName) {
    unsigned offset = 0;

    // TableID
    memcpy((char *)data + offset, &tableId, sizeof(int));
    offset += sizeof(int);

    // TableName
    int size = (int) tableName.size();
    memcpy((char *)data + offset, &size, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, tableName.c_str(), size);
    offset += size;

    // TODO: new added
    // FileName
    size = (int) fileName.size();
    memcpy((char *)data + offset, &size, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, fileName.c_str(), size);
    offset += size;
}

void RelationManager::prepareColumnRecord(char *data, int tableId, AttrType attrType,
                        unsigned columnSize, string attributeName) {
    unsigned offset = 0;

    // TableID
    memcpy((char *)data + offset, &tableId, sizeof(int));
    offset += sizeof(int);

    // ColumnType
    memcpy((char *)data + offset, &attrType, sizeof(int));
    offset += sizeof(int);

    // ColumnSize
    memcpy((char *)data + offset, &columnSize, sizeof(int));
    offset += sizeof(int);

    // AttributeName
    int size = (int) attributeName.size();
    memcpy((char *)data + offset, &size, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, attributeName.c_str(), size);
    offset += size;
}

void RelationManager::getAdmin() {
    admin = true;
}

void RelationManager::yieldAdmin() {
    admin = false;
}

bool RelationManager::isPrivileged(const string &tableName) {
    if (admin) {
        return true;
    }
    if (tableName.compare(TABLES_NAME) == 0 ||
        tableName.compare(COLUMNS_NAME) == 0) {
        return false;
    }
    return true;
}

string RelationManager::getFileName(const string &tableName) {
    return tableName + TABLE_FILE_SUFFIX;
}

RC RelationManager::getTableFileHandle(const string &tableName, FileHandle &fileHandle) {
    RC err;
    if ((err = getCachedTableHandle(tableName, fileHandle)) == ERR_HANDLE_NOT_CACHED) {
        if ((err = _rbfm->openFile(getFileName(tableName), fileHandle)) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }
    return SUCCESSFUL;
}
