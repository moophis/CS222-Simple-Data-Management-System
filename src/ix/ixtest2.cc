#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_2(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Open Index file
    // 2. Insert entry **
    // 3. Disk I/O check of Insertion - CollectCounterValues **
    // 4. Disk I/O check of Scan and getNextEntry - CollectCounterValues **
    // 5. Delete entry **
    // 6. Disk I/O check of Deletion - CollectCounterValues **
    // 7. Delete entry -- when the value is not there: should fail **
    // 8. Close Index file
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 2****" << endl;

    RID rid;
    RC rc;
    unsigned numOfTuples = 1;
    unsigned key = 100;
    rid.pageNum = key;
    rid.slotNum = key+1;
    int age = 18;
	unsigned readPageCount = 0;
	unsigned writePageCount = 0;
	unsigned appendPageCount = 0;
	unsigned readPageCount1 = 0;
	unsigned writePageCount1 = 0;
	unsigned appendPageCount1 = 0;
	unsigned readDiff = 0;
	unsigned writeDiff = 0;
	unsigned appendDiff = 0;
	
	IX_ScanIterator ix_ScanIterator;
	
    // open index file
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    if(rc == success)
    {
        cout << "Index File, " << indexFileName << " Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        return fail;
    }

    // collect counters
	rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    if(rc != success)
    {
        cout << "collectCounterValues() failed." << endl;
        indexManager->closeFile(ixfileHandle);
        return fail;
    }
	
	cout << endl << "Before Insert - readPageCount:" << readPageCount << " writePageCount:" << writePageCount << " appendPageCount:" << appendPageCount << endl;
	
    // insert entry
    for(unsigned i = 0; i < numOfTuples; i++)
    {
        rc = indexManager->insertEntry(ixfileHandle, attribute, &age, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
            indexManager->closeFile(ixfileHandle);
            return fail;
        }
    }

    // collect counters
	rc = ixfileHandle.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1);
	if(rc != success)
    {
        cout << "collectCounterValues() failed." << endl;
        indexManager->closeFile(ixfileHandle);
        return fail;
    }
	cout << "After Insert - readPageCount:" << readPageCount1 << " writePageCount:" << writePageCount1 << " appendPageCount:" << appendPageCount1 << endl;

	readDiff = readPageCount1 - readPageCount;
	writeDiff = writePageCount1 - writePageCount;
	appendDiff = appendPageCount1 - appendPageCount;
	
	cout << "Page I/O count of single insertion - readPage:" << readDiff << " writePageCount:" << writeDiff << " appendPage:" << appendDiff << endl;
	
	if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
		cout << "Insertion should generate some page I/O. The implementation is not correct." << endl;
        indexManager->closeFile(ixfileHandle);		
		return fail;
	} 
	
	
	// collect counters
	rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
	if(rc != success)
    {
        cout << "collectCounterValues() failed." << endl;
        indexManager->closeFile(ixfileHandle);
        return fail;
    }
	cout << endl << "Before scan - readPageCount:" << readPageCount << " writePageCount:" << writePageCount << " appendPageCount:" << appendPageCount << endl;
	
	// Conduct a scan
	rc = indexManager->scan(ixfileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan!" << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

	// should be one record
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << "Returned rid from a scan: " << rid.pageNum << " " << rid.slotNum << endl;
    }

    // collect counters
	rc = ixfileHandle.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1);
	if(rc != success)
    {
        cout << "collectCounterValues() failed." << endl;
        indexManager->closeFile(ixfileHandle);
        return fail;
    }
	cout << "After scan - readPageCount:" << readPageCount1 << " writePageCount:" << writePageCount1 << " appendPageCount:" << appendPageCount1 << endl;
	
	readDiff = readPageCount1 - readPageCount;
	writeDiff = writePageCount1 - writePageCount;
	appendDiff = appendPageCount1 - appendPageCount;
	
	cout << "Page I/O count of scan - readPage:" << readDiff << " writePageCount:" << writeDiff << " appendPage:" << appendDiff << endl;
	
	if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
		cout << "Scan should generate some page I/O. The implementation is not correct." << endl;
        indexManager->closeFile(ixfileHandle);		
		return fail;
	} 
	
	// collect counters
	rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
	if(rc != success)
    {
        cout << "collectCounterValues() failed." << endl;
        indexManager->closeFile(ixfileHandle);
        return fail;
    }
	cout << endl << "Before Delete - readPageCount:" << readPageCount << " writePageCount:" << writePageCount << " appendPageCount:" << appendPageCount << endl;
	
    // delete entry
    rc = indexManager->deleteEntry(ixfileHandle, attribute, &age, rid);
    if(rc != success)
    {
        cout << "Failed Deleting Entry..." << endl;
        indexManager->closeFile(ixfileHandle);
        return fail;
    }

    // collect counters
	rc = ixfileHandle.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1);
	if(rc != success)
    {
        cout << "collectCounterValues() failed." << endl;
        indexManager->closeFile(ixfileHandle);
        return fail;
    }
	cout << "After Delete - readPageCount:" << readPageCount1 << " writePageCount:" << writePageCount1 << " appendPageCount:" << appendPageCount1 << endl;

	readDiff = readPageCount1 - readPageCount;
	writeDiff = writePageCount1 - writePageCount;
	appendDiff = appendPageCount1 - appendPageCount;

	cout << "Page I/O count of single delete - readPage:" << readDiff << " writePageCount:" << writeDiff << " appendPage:" << appendDiff << endl << endl;
	
	if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
		cout << "Deletion should generate some page I/O. The implementation is not correct." << endl;
        indexManager->closeFile(ixfileHandle);		
		return fail;
	} 

    // delete entry again
    rc = indexManager->deleteEntry(ixfileHandle, attribute, &age, rid);
    if(rc == success) //This time it should NOT give success because entry is not there.
    {
        cout << "Entry deleted again...failure" << endl;
        return fail;
    }

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        return fail;
    }

    return success;

}

int main()
{
    //Global Initializations
    indexManager = IndexManager::instance();

	const string indexFileName = "age_idx";
	Attribute attrAge;
	attrAge.length = 4;
	attrAge.name = "age";
	attrAge.type = TypeInt;

    RC result = testCase_2(indexFileName, attrAge);
    if (result == success) {
    	cout << "IX_Test Case 2 passed" << endl;
    	return success;
    } else {
    	cout << "IX_Test Case 2 failed" << endl;
    	return fail;
    }

}

