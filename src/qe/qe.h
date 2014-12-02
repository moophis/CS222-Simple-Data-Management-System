#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <cfloat>
#include <climits>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;

enum {
    ERR_NO_INPUT     = -401,     // error: empty data input
    ERR_NO_ATTR      = -402,     // error: cannot find attribute
    ERR_INV_TYPE     = -403,     // error: invalid type
};

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by
//                 the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs[i].name);
            }

            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
//            __trace();
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

//            __trace();
//            cout << "IndexScan: attrName: " << attrName << endl;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
//            __trace();
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        Iterator *_iterator;
        Condition _condition;
};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator *_iterator;
        vector<string> _attrNames;
};


// Partition builder: used by GHJoin partitioning phase.
// On its construction, each partition only has one page of buffer.
class PartitionBuilder {
public:
    PartitionBuilder(const string &partitionName, const vector<Attribute> &attrs);
    ~PartitionBuilder();

    // Insert tuple into the partition. Buffer page will be automatically
    // be flushed when it's filled.
    RC insertTuple(void *tuple);

    // Flush remaining buffer (used in the last step)
    RC flushLastPage();

    void getAttributes(vector<Attribute> &attrs);
    string getPartitionName();

private:
    RC init();  // create the partition file and initialize file handle

private:
    string _fileName;
    vector<Attribute> _attrs;
    FileHandle _fileHandle;
    RecordBasedFileManager *_rbfm;
    SpaceManager *_sm;
    PagedFileManager *_pfm;
    char _buffer[PAGE_SIZE];
};

// Partition reader: used by GHJoin probing phase.
// On its retrieval, all pages will be buffered.
class PartitionReader {
public:
    PartitionReader(const string &partitionName, const vector<Attribute> &attrs);
    ~PartitionReader();

    // Get next tuple from the partition while caching it in memory
    RC getNextTuple(void *tuple, RID &rid, unsigned &size);
    // Get tuple from cache (should first continuously call getNextTuple()
    // until reaching the end.
    RC getTupleFromCache(void *tuple, unsigned &size, const RID &rid);
    void getAttributes(vector<Attribute> &attrs);
    unsigned getPageCount();
    string getPartitionName();

private:
    RC init();  // open the partition file and get the file handle

private:
    string _fileName;
    vector<Attribute> _attrs;
    FileHandle _fileHandle;
    unsigned _pageCount;
    unsigned _curPageNum;   // current page number
    unsigned _curSlotNum;   // current slot number

    RecordBasedFileManager *_rbfm;
    SpaceManager *_sm;
    PagedFileManager *_pfm;
    char **_buffer;     // to hold the whole partition
};


class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      );
      ~GHJoin();

      RC getNextTuple(void *data);
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const;

      enum IterType { LEFT = 0, RIGHT };

    private:
        RC partition(Iterator *iter, IterType iterType);
        void allocatePartition(Iterator *iter, IterType iterType);
        void deallocatePartition(IterType iterType);
        string getPartitionName(IterType iterType, unsigned num);
        // 1st hashing
        unsigned hash1(char *value, unsigned size);
        // 2nd hashing
        unsigned hash2(char *value, unsigned size);

    private:
        // Use join number to handle multiple joins in one query
        static int _joinNumberGlobal;
        int _joinNumber;

        Iterator *_leftIn;
        Iterator *_rightIn;
        vector<Attribute> _leftAttrs;
        vector<Attribute> _rightAttrs;
        Condition _condition;
        unsigned _numPartitions;
        vector<PartitionBuilder *> _leftPartitions;
        vector<PartitionBuilder *> _rightPartitions;
        PartitionReader * _leftReader;
        PartitionReader * _rightReader;
        unsigned _curPartition; // the current partition to read
        unordered_map<unsigned, vector<RID> > _hashMap;  // the hash map for the second hashing
};


class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
        ){};
        ~BNLJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){};
        ~INLJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};


class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory for graduate teams only
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone. 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op,              // Aggregate operation
                  const unsigned numPartitions // Number of partitions for input (decided by the optimizer)
        ) {};

        ~Aggregate(){};

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        RC process();

    private:
        Iterator *_iterator;
        Attribute _aggAttr;
        AggregateOp _op;

        bool _gotResult;      // Check whether we have got the result

        int _count;
        int _intSum;
        float _floatSum;
        int _intMin;
        float _floatMin;
        int _intMax;
        float _floatMax;
};

#endif
