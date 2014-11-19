#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_3(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Open Index File
    // 2. Insert Entries
    // 3. PrintIndexEntriesInAPage **
    // 4. Destroy Index File **
    // 5. Open Index File -- should fail
    cout << endl << "****In Test Case 3****" << endl;

    RC rc;
    IXFileHandle ixfileHandle;
    IXFileHandle ixfileHandle1;
    IX_ScanIterator ix_ScanIterator;
	unsigned numberOfPagesFromFunction = 0;
	unsigned key = 10;
	RID rid;

    // open index file
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

	// Insert entries
    for(unsigned i = 0; i < 20; i++)
    {
        rid.pageNum = i;
        rid.slotNum = i+1;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
            indexManager->closeFile(ixfileHandle);
            return fail;
        }
    }

	// Get number of primary pages
    rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
    if(rc != success)
    {
    	cout << "getNumberOfPrimaryPages() failed." << endl;
    	indexManager->closeFile(ixfileHandle);
		return fail;
    }

	// Print Entries in each page
	for (unsigned i = 0; i < numberOfPagesFromFunction; i++) {
		rc = indexManager->printIndexEntriesInAPage(ixfileHandle, attribute, i);
		if (rc != success) {
        	cout << "printIndexEntriesInAPage() failed." << endl;
			indexManager->closeFile(ixfileHandle);
			return fail;
		}
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
    
    // destroy index file
    rc = indexManager->destroyFile(indexFileName);
    if(rc != success)
    {
        cout << "Failed Destroying Index File..." << endl;
        return fail;
    }

    // open the destroyed index
    rc = indexManager->openFile(indexFileName, ixfileHandle1);
    if(rc == success) //should not work now
    {
        cout << "Index opened again...failure" << endl;
        indexManager->closeFile(ixfileHandle1);
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

    RC result = testCase_3(indexFileName, attrAge);;
    if (result == success) {
    	cout << "IX_Test Case 3 passed" << endl;
    	return success;
    } else {
    	cout << "IX_Test Case 3 failed" << endl;
    	return fail;
    }

}

