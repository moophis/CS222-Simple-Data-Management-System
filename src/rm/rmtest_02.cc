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

void TEST_RM_2(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Delete Tuple **
    // 3. Read Tuple
    // Additional: scan index after record insertion and deletion
    cout << "****In Test Case 2****" << endl;

    RID rid;
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);

    // Test Insert the Tuple
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    cout << "Insert Data:" << endl;
    printTuple(tuple, tupleSize);
    RC rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);

    // Test scan index
    printScanResult(tableName);

    // Test Delete Tuple
    rc = rm->deleteTuple(tableName, rid);
    assert(rc == success);
    cout<< "delete data done"<<endl;

    // Test Read Tuple
    memset(returnedData, 0, 100);
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc != success);

    cout << "After Deletion." << endl;
    // Test scan index after deletion
    printScanResult(tableName);

    // Compare the two memory blocks to see whether they are different
    if (memcmp(tuple, returnedData, tupleSize) != 0)
    {
        cout << "****Test case 2 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 2 failed****" << endl << endl;
    }

    free(tuple);
    free(returnedData);
    return;
}

int main()
{
    cout << endl << "Test Delete Tuple .." << endl;

    // Delete Tuple
    TEST_RM_2("tbl_employee", 6, "Victor", 22, 180.2, 6000);

    return 0;
}
