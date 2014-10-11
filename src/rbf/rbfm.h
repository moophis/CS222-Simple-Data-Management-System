#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <map>
#include <set>
//#include <unordered_map>
//#include <unordered_set>

#include "../rbf/pfm.h"

using namespace std;


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;

// enum status code
enum {
    ERR_UNKNOWN_TYPE  = -100,   // error: unknown attribute type
    ERR_FORMAT        = -101,   // error: wrong record format
};

/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

  // "data" follows the same format as RecordBasedFileManager::insertRecord()
  RC getNextRecord(RID &rid, void *data) { return RBFM_EOF; };
  RC close() { return -1; };
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

  // This method will be mainly used for debugging/testing
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

  RC countRecordSize(const vector<Attribute> &recordDescriptor, const void *data, unsigned &size);

/**************************************************************************************************************************************************************
***************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
***************************************************************************************************************************************************************
***************************************************************************************************************************************************************/
  RC deleteRecords(FileHandle &fileHandle);

  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the rid does not change after update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);

  RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);


// Extra credit for part 2 of the project, please ignore for part 1 of the project
public:

  RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);


protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;

  static PagedFileManager *_pfm_manager;
};

// Manager free space for each page of a database file
enum {
  ERR_BAD_HANDLE        = -201,  // error: FileHand is bad
  ERR_BAD_DATA          = -202,  // error: bad data
  ERR_SIZE_TOO_LARGE    = -203,  // error: request too large data
  ERR_RECORD_NOT_FOUND  = -204,  // error: cannot find the record
};

class SpaceManager {
public:
  typedef map<int, set<int> > FreeSpaceMap;    // <free space size, corresponding locations (page #)>

  static SpaceManager *instance();
  static void *getPageBuffer();
  RC bufferSizeInfo(const string &fileName, FileHandle &fileHandle);
  RC clearSizeInfo(const string &fileName, FileHandle &fileHandle); // TODO
  RC allocateSpace(const string &fileName, FileHandle &fileHandle, int spaceSize, int &pageNum);
  RC deallocateSpace(const string &fileName, FileHandle &fileHandle, int pageNum, int startPosition); /// TODO
  void printFreeSpaceMap();   // Print out the map for debugging purposes
  void updateFreeSpaceMap(const string &fileName, int pageNum, int size);
  void clearFreeSpaceMap(const string &fileName);

  // TODO: for bufferSizeInfo(), readRecord()
  unsigned short getFreePtr(void *page);
  unsigned short getSlotCount(void *page);
  short getSlotStartPos(void *page, int slotNum);
  unsigned short getSlotLength(void *page, int slotNum);
  void setFreePtr(void *page, unsigned short data);
  void setSlotCount(void *page, unsigned short data);
  void setSlotStartPos(void *page, int slotNum, short data);
  void setSlotLength(void *page, int slotNum, unsigned short data);
  void setSlot(void *page, int slotNum, short start, unsigned short length);
  void writeRecord(void *page, const void *data, unsigned short start, unsigned size);
  void readRecord(const void *page, void *data, unsigned short start, unsigned size);

  int getMetadataSize(int slotNum);
  // Find whether there are still allocated slots yet used. If so, return the first slot #
  bool hasFreeExistingSlot(void *page, unsigned short slotNum, unsigned short &firstFreeSlot);
  void initCleanPage(void *page);

private:
  // Variable sizes within metadata (in byte)
  enum {
    FREE_PTR_LEN   = 2,
    SLOT_NUM_LEN   = 2,
    SLOT_START_LEN = 2,
    SLOT_LEN_LEN   = 2,
  };
  static map<string, FreeSpaceMap> __freeSpace;       // <file name, free space map>
  static SpaceManager *_sp_manager;   // SpaceManager instance
  static void *__buffer;              // the page buffer used to store a page temporarily

protected:
  SpaceManager();
  ~SpaceManager();
};

#endif
