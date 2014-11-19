#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_1(const string &indexFileName)
{
    // Functions tested
    // 1. Create Index File **
    // 2. Open Index File **
    // 3. Get number of Primary Pages **
    // 4. Get number of All Pages including overflow pages **
    // 5. Create Index File -- when index file is already created **
    // 6. Close Index File **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 1****" << endl;

    RC rc;
    IXFileHandle ixfileHandle;
    unsigned numberOfPages = 4;
	unsigned numberOfPagesFromFunction = 0;
	
    // create index file
    rc = indexManager->createFile(indexFileName, numberOfPages);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        return fail;
    }

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

    // Get number of all pages
    rc = indexManager->getNumberOfAllPages(ixfileHandle, numberOfPagesFromFunction);
    if(rc == success)
    {
        if (numberOfPagesFromFunction < numberOfPages) {
        	cout << "The number of initially constructed pages is not correct." << endl;
        	return fail;
        }
    }
    else
    {
        cout << "Could not get the number of pages." << endl;
        return fail;
    }
    
    // Get number of primary pages
    numberOfPagesFromFunction = 0;
    rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
    if(rc == success)
    {
        if (numberOfPagesFromFunction != numberOfPages) {
        	cout << "The number of initially constructed pages is not correct." << endl;
        	return fail;
        }
    }
    else
    {
        cout << "Could not get the number of pages." << endl;
        return fail;
    }	
	
    // Try to create duplicate index file
    rc = indexManager->createFile(indexFileName, numberOfPages);
    if(rc != success)
    {
        cout << "Duplicate Index File not Created -- correct!" << endl;
    }
    else
    {
        cout << "Duplicate Index File Created -- failure..." << endl;
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

    RC result = testCase_1(indexFileName);
    if (result == success) {
    	cout << "IX_Test Case 1 passed" << endl;
    	return success;
    } else {
    	cout << "IX_Test Case 1 failed" << endl;
    	return fail;
    }

}

