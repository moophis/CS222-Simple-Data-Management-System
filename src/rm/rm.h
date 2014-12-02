
#ifndef _rm_h_
#define _rm_h_
#include <iostream>
#include <sstream>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <utility>
#include <cstddef>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
  RBFM_ScanIterator rbfm_ScanIterator;
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  void init(RBFM_ScanIterator &rbfm_ScanIterator) {
      this->rbfm_ScanIterator = rbfm_ScanIterator;
  }

  RC getNextTuple(RID &rid, void *data) {
      return rbfm_ScanIterator.getNextRecord(rid, data);
  }

  RC close() {
      return rbfm_ScanIterator.close();
  }
};

class RM_IndexScanIterator {

public:
  RM_IndexScanIterator() {}   // Constructor
  ~RM_IndexScanIterator() {} // Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  // Get next matching entry
  void init(IX_ScanIterator &ix_ScanIterator) {
      this->ix_ScanIterator = ix_ScanIterator;
  }

  RC getNextEntry(RID &rid, void *key) {
      return ix_ScanIterator.getNextEntry(rid, key);
  }

  // Terminate index scan
  RC close() {
      return ix_ScanIterator.close();
  }

private:
  IX_ScanIterator ix_ScanIterator;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuples(const string &tableName);

  RC deleteTuple(const string &tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  RC reorganizePage(const string &tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  // Index related functions
public:
  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);
private:
  RC insertIndexEntry(const int &tableId, const Attribute &attribute, const void *key, const RID &rid);
  RC deleteIndexEntry(const int &tableId, const Attribute &attribute, const void *key, const RID &rid);
  // Insert/delete all index entries associated with one new/old record content
  RC insertIndexEntries(const string &tableName, const vector<Attribute> &attrs, const RID &rid);
  RC deleteIndexEntries(const string &tableName, const vector<Attribute> &attrs, const RID &rid);
  // Retrieve column name from either [Attribute] or [Relation.Attribute]
  string retrieveColumnName(const string &attributeName);

// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC reorganizeTable(const string &tableName);

protected:
  RelationManager();
  ~RelationManager();

private:
  // Open or create "Tables" or "Columns" or "Indexes"
  RC initCatalogFile(const string &tableName, FileHandle &handle, bool &newFile);
  // Initialize schema (attribute vector) for table "Tables", "Columns" and "Indexes"
  void initCatalogSchema();

  // Prepare catalog data for a table
  void prepareTableRecord(char *data, int tableId, string tableName, string fileName);
  void prepareColumnRecord(char *data, int tableId, AttrType attrType,
                           unsigned columnSize, string attributeName);
  void prepareIndexRecord(char *data, int tableId, AttrType attrType,
                           unsigned keySize, string keyName);   // TODO

  // Wire metadata for a new created table (writing records in Tables, Columns)
  // And update maps
  RC wireTableMetadata(const string &tableName, const int tableId, const vector<Attribute> &recordAttributes);
  // Drop metadata as well as entries in maps
  RC dropTableMetadata(const string &tableName);

  // TODO
  // Wire metadata for a new created index (writing records in Indexes)
  // And update maps
  RC wireIndexMetadata(const string &indexName, const int &tableId, const Attribute &attribute);
  RC dropIndexMetadata(const string &indexName, const int &tableId, const string &attributeName);

  // Register/Deregister table/index maps by inserting/removing table information in maps
  void registerTableMapping(const string &tableName, const int tableId, const RID &rid);
  RC deregisterTableMapping(const string &tableName, RID &rid);

  // Append attribute to the schema map
  void appendAttributeMapping(const int tableId, const Attribute &attribute, const RID &rid);
  RC dropAttributesMapping(const int tableId, vector<RID> &rids);
  void appendIndexKeyMapping(const int tableId, const Attribute &attribute, const RID &rid);
  RC dropIndexKeyMapping(const int tableId, const string &attributeName, RID &rid);

  // Check whether an index exists
  bool doesIndexExist(const int &tableId, const string &attributeName);

  // Debug: print catalog maps
  void printCatalogMaps();

  // Get table id from tableName
  RC getTableId(const string &tableName, int &tableId);
  RC getTableMetadataRID(const string &tableName, RID &rid);
  // Get table attributes info from table id
  RC getTableAttributes(const int tableId, vector<pair<Attribute, RID> > &attrPairs);

  // Cache / De-cache (index) file handle for each table file
  // Cache the handle when the table it handles is firstly used after system is on.
  void cacheTableHandle(const string &tableName, FileHandle &handle);
  void dropTableHandle(const string &tableName);
  RC getCachedTableHandle(const string &tableName, FileHandle &handle);

  void cacheIndexHandle(const string &indexName, IXFileHandle &handle);
  void dropIndexHandle(const string &indexName);
  RC getCachedIndexHandle(const string &indexName, IXFileHandle &handle);

  // Debug: print handle map
  void printTableHandleMap();
  void printFileHandle(FileHandle &handle);
  void printIndexHandleMap();
  void printIXFileHandle(IXFileHandle &handle);

  // Buffer the table-attribute mapping from catalog files
  RC bufferMappings();

  // Admin privileged operations
  void getAdmin();
  void yieldAdmin();
  bool isPrivileged(const string &tableName);  // check whether the operation is privileged

  // Get table / index file name from table name
  string getTableFileName(const string &tableName);
  string getIndexName(const string &attributeName, const int &tableId);
  string getFullAttriuteName(const string &tableName, const string &keyName);
  string getIndexFileName(const string &indexName);
  // Get table file handle (first try to retrieve from cache, if not exist, open the file)
  RC getTableFileHandle(const string &tableName, FileHandle &fileHandle);
  // Get index file handle (first try to retrieve from cache, if not exist, open the file)
  RC getIndexFileHandle(const string &indexName, IXFileHandle &fileHandle);

  // Get Attribute object from attribute string
  RC getAttributeFromString(const string &tableName, const string &attributeName, Attribute &attr);

  static RelationManager *_rm;

  static RecordBasedFileManager *_rbfm;

  static IndexManager *_ixm;

  // A map from table name to its corresponding file handle
  unordered_map<string, FileHandle> tableHandles;

  // A map from index name to its corresponding file handle
  unordered_map<string, IXFileHandle> indexHandles;

  // A map from the table id to the table schema and its RID in "Columns"
  unordered_map<int, vector<pair<Attribute, RID> > > schemaMap;

  // A map from the table id to its associated index keys and their RID in "Indexes"
  unordered_map<int, vector<pair<Attribute, RID> > > indexMap;

  // A map from the table name to its id and its RID in "Tables"
  unordered_map<string, pair<int, RID> > tableNameMap;

  // Max table id
  int maxTableId;

  // Whether an operation is privileged
  bool admin = false;

  // Pre-defined catalog schemas
  vector<Attribute> tablesSchema;
  vector<Attribute> columnsSchema;
  vector<Attribute> indexesSchema;
};

// error message in rm layer
enum {
    ERR_HANDLE_NOT_CACHED = -301,  // error: cannot find cached file handle from the map
    ERR_NO_PERMISSION     = -302,  // error: the current operation is not permitted
    ERR_NO_MAP_ENTRY      = -303,  // error: cannot find the map entry
    ERR_NO_SUCH_INDEX     = -304,  // error: cannot find the index
    ERR_INDEX_EXISTS      = -305,  // error: the index already exists
};

#endif
