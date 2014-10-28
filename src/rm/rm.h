
#ifndef _rm_h_
#define _rm_h_
#include <iostream>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <utility>

#include "../rbf/rbfm.h"

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
  };
  RC close() {
      return rbfm_ScanIterator.close();
  };
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


// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC reorganizeTable(const string &tableName);



protected:
  RelationManager();
  ~RelationManager();

private:
  // Open or create ".tables" or ".columns"
  RC initCatalogFile(const string &tableName, FileHandle &handle, bool &newFile);
  // Initialize schema (attribute vector) for table "tables" and "columns"
  void initCatalogSchema();

  // Prepare catalog data for a table
  void prepareTableRecord(char *data, int tableId, string tableName, string fileName);
  void prepareColumnRecord(char *data, int tableId, AttrType attrType,
                           unsigned columnSize, string attributeName);
  // Wire metadata for a new created table (writing records in .tables and .columns)
  // And update maps
  RC wireTableMetadata(const string &tableName, const int tableId, const vector<Attribute> &recordAttributes);
  // Drop metadata as well as entries in maps
  RC dropTableMetadata(const string &tableName);

  // Register/Deregister table maps by inserting/removing table information in maps
  void registerTableMapping(const string &tableName, const int tableId, const RID &rid);
  void deregisterTableMapping(const string &tableName, RID &rid);
  // Append attribute to the schema map
  void appendAttributeMapping(const int tableId, const Attribute &attribute, const RID &rid);
  void dropAttributesMapping(const int tableId, vector<RID> &rids);
  // Debug: print catalog maps
  void printCatalogMaps();

  // Get table id from tableName
  RC getTableId(const string &tableName, int &tableId);
  RC getTableMetadataRID(const string &tableName, RID &rid);
  // Get table attributes info from table id
  RC getTableAttributes(const int tableId, vector<pair<Attribute, RID> > &attrPairs);

  // Cache / De-cache file handle for each table file
  // Cache the handle when the table it handles is firstly used after system is on.
  void cacheTableHandle(const string &tableName, FileHandle &handle);
  void dropTableHandle(const string &tableName);
  RC getCachedTableHandle(const string &tableName, FileHandle &handle);
  // Debug: print handle map
  void printTableHandleMap();
  void printFileHandle(FileHandle &handle);

  // Buffer the table-attribute mapping from catalog files
  RC bufferMappings();

  // Admin privileged operations
  void getAdmin();
  void yieldAdmin();
  bool isPrivileged(const string &tableName);  // check whether the operation is privileged

  // Get file name from table name
  string getFileName(const string &tableName);
  // Get file handle (first try to retrieve from cache, if not exist, open the file)
  RC getTableFileHandle(const string &tableName, FileHandle &fileHandle);

  static RelationManager *_rm;

  static RecordBasedFileManager *_rbfm;

  // A map from table name to its corresponding file handler
  unordered_map<string, FileHandle> tableHandles;

  // A map from the table id to the table schema and its RID in ".columns"
  unordered_map<int, vector<pair<Attribute, RID> > > schemaMap;

  // A map from the table name to its id and its RID in ".tables"
  unordered_map<string, pair<int, RID> > tableNameMap;

  // Max table id
  int maxTableId;

  // Whether an operation is privileged
  bool admin = false;

  // Pre-defined catalog schemas
  vector<Attribute> tablesSchema;
  vector<Attribute> columnsSchema;

};

// error message in rm layer
enum {
    ERR_HANDLE_NOT_CACHED = -301,  // error: cannot find cached file handle from the map
    ERR_NO_PERMISSION     = -302,  // error: the current operation is not permitted
    ERR_NO_MAP_ENTRY      = -303,  // error: cannot find the map entry
};

#endif
