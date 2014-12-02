#include "test_util.h"

void printScanResult(const string &tableName) {
    cout << "Entries in Index EmpName" << endl;
    scanIndex(tableName, "EmpName", TypeVarChar);

    cout << "Entries in Index Age" << endl;
    scanIndex(tableName, "Age", TypeInt);

    cout << "Entries in Index Height" << endl;
    scanIndex(tableName, "Height", TypeReal);

    cout << "Entries in Index Salary" << endl;
    scanIndex(tableName, "Salary", TypeInt);
}

void TEST_RM_1(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions tested
    // 1. Insert Tuple **
    // 2. Read Tuple **
    // NOTE: "**" signifies the new functions being tested in this test case.
    // Additional: create indexes, scan index
    cout << "****In Test Case 1****" << endl;

    RC rc;
    RID rid;
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);

    // Create indexes
    rc = rm->createIndex(tableName, "EmpName");
    assert(rc == success);
    rc = rm->createIndex(tableName, "Age");
    assert(rc == success);
    rc = rm->createIndex(tableName, "Height");
    assert(rc == success);
    rc = rm->createIndex(tableName, "Salary");
    assert(rc == success);

    // Insert a tuple into a table
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    cout << "Insert Data:" << endl;
    printTuple(tuple, tupleSize);
    rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);

    cout << "Data inserted at <" << rid.pageNum << ", " << rid.slotNum << ">" << endl;

    // Given the rid, read the tuple from table
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success);

    cout << "Returned Data:" << endl;
    printTuple(returnedData, tupleSize);

    // Scan from each index
    printScanResult(tableName);

//    // Delete table
//    rc = rm->deleteTable(tableName);
//    assert(rc == success);
//    cout << "Delete table done" << endl;

    // Compare whether the two memory blocks are the same
    if(memcmp(tuple, returnedData, tupleSize) == 0)
    {
        cout << "****Test case 1 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 1 failed****" << endl << endl;
    }

    free(tuple);
    free(returnedData);
    return;
}

int main()
{
    cout << endl << "Test Insert/Read Tuple .." << endl;

    // Insert/Read Tuple
    TEST_RM_1("tbl_employee", 6, "Peters", 24, 170.1, 5000);

    return 0;
}
