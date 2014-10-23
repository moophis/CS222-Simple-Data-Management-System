#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pfm.h"
#include "rbfm.h"

using namespace std;

const int success = 0;
unsigned total = 0;

// Check if a file exists
bool FileExists(string fileName) {
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) == 0)
		return true;
	else
		return false;
}

int RBFTest_12(RecordBasedFileManager *rbfm) {
	// Functions Tested:
	// 1. Create File
	// 2. Open File
	// 3. Get Number of Pages
	// 4. Read a Page that does not exist - should fail
	// 5. Write a Page that does not exist - should fail
	// 6. Close File
	cout << "****In RBF Test Case 12****" << endl;

	RC rc;
	string fileName = "test12";

	// Create a file named "test12"
	rc = rbfm->createFile(fileName.c_str());
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	if (FileExists(fileName.c_str())) {
		cout << "File " << fileName << " has been created." << endl << endl;
	} else {
		cout << "Failed to create file!" << endl;
		return -1;
	}

	// Open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(fileName.c_str(), fileHandle);
	if (rc != success) {
		return -1;
	}
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	// Get the number of pages in the test file
	unsigned count = fileHandle.getNumberOfPages();

	// Read a non-exist page
	unsigned pageToAccess = count + 100;
	void *buffer = malloc(PAGE_SIZE);
	rc = fileHandle.readPage(pageToAccess, buffer);
	if (rc == success) {
		cout << "This readPage test should fail. However, it returned a success RC." << endl;
		return -1;
	}
	assert(rc != success);


	// Update a non-exist page
	void *data = malloc(PAGE_SIZE);
	for (unsigned i = 0; i < PAGE_SIZE; i++) {
		*((char *) data + i) = i % 10 + 32;
	}
	rc = fileHandle.writePage(pageToAccess, data);
	if (rc == success) {
		cout << "This writePage test should fail. However, it returned a success RC." << endl;
		return -1;
	}
	assert(rc != success);

	// Close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	return 0;
}

int main() {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); // To test the functionality of the record-based file manager

	remove("test12");

	int rc = RBFTest_12(rbfm);
	if (rc == 0) {
		cout << "Test Case 12 Passed!" << endl << endl;
		total += 4;
	} else {
		cout << "Test Case 12 Failed!" << endl << endl;
	}

	cout << "Score for Test Case 12: " << total << " / 4" << endl;

	return 0;
}
