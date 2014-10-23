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
int total = 0;

// Check if a file exists
bool FileExists(string fileName) {
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) == 0)
		return true;
	else
		return false;
}

// Record Descriptor for TwitterUser
void createRecordDescriptorForTwitterUser(vector<Attribute> &recordDescriptor) {

	Attribute attr;
	attr.name = "userid";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "screen_name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 300;
	recordDescriptor.push_back(attr);

	attr.name = "lang";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 300;
	recordDescriptor.push_back(attr);

	attr.name = "friends_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "statuses_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "username";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 300;
	recordDescriptor.push_back(attr);

	attr.name = "followers_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "satisfaction_score";
	attr.type = TypeReal;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

}

// Record Descriptor for TwitterUser
void createRecordDescriptorForTwitterUser2(vector<Attribute> &recordDescriptor) {

	Attribute attr;
	attr.name = "userid";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "screen_name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 800;
	recordDescriptor.push_back(attr);

	attr.name = "lang";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 800;
	recordDescriptor.push_back(attr);

	attr.name = "friends_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "statuses_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "username";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 800;
	recordDescriptor.push_back(attr);

	attr.name = "followers_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "satisfaction_score";
	attr.type = TypeReal;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

}

void prepareLargeRecordForTwitterUser(const int index, void *buffer,
		int *size) {
	int offset = 0;

	int count = index % 200 + 1;
	int text = index % 26 + 97;
	string suffix(count, text);
	for (int i = 0; i < count - 1; i++) {
		suffix += text;
	}

	// userid = index
	memcpy((char *) buffer + offset, &index, sizeof(int));
	offset += sizeof(int);

	// screen_name
	string screen_name = "NilaMilliron@" + suffix;
	int screen_nameLength = screen_name.length();

	if (screen_nameLength > 300) {
	    cout << "screen_nameLength: " << screen_nameLength << endl;
	}

	memcpy((char *) buffer + offset, &screen_nameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, screen_name.c_str(), screen_nameLength);
	offset += screen_nameLength;

	// lang
	string lang = "En " + suffix;
	int langLength = screen_name.length();

	if (langLength > 300) {
	    cout << "langLength: " << langLength << endl;
	}

	memcpy((char *) buffer + offset, &langLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, lang.c_str(), langLength);
	offset += langLength;

	// friends_count
	int friends_count = count - 1;

	memcpy((char *) buffer + offset, &friends_count, sizeof(int));
	offset += sizeof(int);

	// statuses_count
	int statuses_count = count + 1;

	memcpy((char *) buffer + offset, &statuses_count, sizeof(int));
	offset += sizeof(int);

	// username
	string username = "Nilla Milliron " + suffix;
	int usernameLength = username.length();

	if (usernameLength > 300) {
	    cout << "usernameLength: " << usernameLength << endl;
	}

	memcpy((char *) buffer + offset, &usernameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, username.c_str(), usernameLength);
	offset += usernameLength;

	// followers_count
	int followers_count = count + 2;

	memcpy((char *) buffer + offset, &followers_count, sizeof(int));
	offset += sizeof(int);

	// satisfaction_score
	float satisfaction_score = (float) count + 0.1;

	memcpy((char *) buffer + offset, &satisfaction_score, sizeof(float));
	offset += sizeof(float);

	*size = offset;
}

bool compareFileSizes(string fileName1, string fileName2) {
	streampos s1, s2;
	ifstream in1(fileName1.c_str(), ifstream::in | ifstream::binary);
	in1.seekg(0, ifstream::end);
	s1 = in1.tellg();

	ifstream in2(fileName2.c_str(), ifstream::in | ifstream::binary);
	in2.seekg(0, ifstream::end);
	s2 = in2.tellg();

	cout << "File 1 size: " << s1 << endl;
	cout << "File 2 size: " << s2 << endl;

	if (s1 != s2) {
		return false;
	}
	return true;
}
ifstream::pos_type filesize(const char* filename) {
	std::ifstream in(filename, std::ifstream::in | std::ifstream::binary);
	in.seekg(0, std::ifstream::end);
	return in.tellg();
}

int RBFTest_16(RecordBasedFileManager *rbfm) {
	// Functions tested
	// 1. Create Two Record-Based File
	// 2. Open Two Record-Based File
	// 3. Insert Multiple Records Into Two files
	// 4. Close Two Record-Based File
	// 5. Compare The File Sizes
	// 6. Destroy Two Record-Based File
	cout << "****In RBF Test Case 16****" << endl;

	RC rc;
	string fileName1 = "test16a";
	string fileName2 = "test16b";

	// Create a file named "test16a"
	rc = rbfm->createFile(fileName1.c_str());
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	if (FileExists(fileName1.c_str())) {
		cout << "File " << fileName1 << " has been created." << endl;
	} else {
		cout << "Failed to create file!" << endl;
		return -1;
	}

	// Create a file named "test16b"
	rc = rbfm->createFile(fileName2.c_str());
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	if (FileExists(fileName2.c_str())) {
		cout << "File " << fileName2 << " has been created." << endl;
	} else {
		cout << "Failed to create file!" << endl;
		return -1;
	}

	// Open the file "test16a"
	FileHandle fileHandle1;
//	rc = rbfm->openFile(fileName1.c_str(), fileHandle1);
	rc = rbfm->openFile(fileName1, fileHandle1);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	// Open the file "test16b"
	FileHandle fileHandle2;
//	rc = rbfm->openFile(fileName2.c_str(), fileHandle2);
	rc = rbfm->openFile(fileName2, fileHandle2);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	RID rid;
	void *record = malloc(2000);
	int numRecords = 10000;

	vector<Attribute> recordDescriptor1;
	createRecordDescriptorForTwitterUser(recordDescriptor1);

	vector<Attribute> recordDescriptor2;
	createRecordDescriptorForTwitterUser2(recordDescriptor2);

	// Insert 10000 records into file
	for (int i = 0; i < numRecords; i++) {
		// Test insert Record
		int size = 0;
		memset(record, 0, 2000);
		prepareLargeRecordForTwitterUser(i, record, &size);

		rc = rbfm->insertRecord(fileHandle1, recordDescriptor1, record, rid);
		if (rc != success) {
		    cout << "insert failed 1, i = " << i << endl;
			return -1;
		}
		assert(rc == success);

		rc = rbfm->insertRecord(fileHandle2, recordDescriptor2, record, rid);
		if (rc != success) {
		    cout << "insert failed 2, i = " << i << endl;
			return -1;
		}
		assert(rc == success);
	}
	// Close the file "test16a"
	rc = rbfm->closeFile(fileHandle1);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	// Close the file "test16b"
	rc = rbfm->closeFile(fileHandle2);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);
	free(record);

	bool equalSizes = compareFileSizes(fileName1, fileName2);

	rc = rbfm->destroyFile(fileName1.c_str());
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	if (FileExists(fileName1.c_str())) {
		cout << "Failed to destroy file!" << endl;
		return -1;
	}

	rc = rbfm->destroyFile(fileName2.c_str());
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	if (FileExists(fileName2.c_str())) {
		cout << "Failed to destroy file!" << endl;
		return -1;
	}

	if (!equalSizes) {
		cout << "Files are of different sizes" << endl;
		return -1;
	}
	return 0;
}

int main() {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); // To test the functionality of the record-based file manager

	remove("test16a");
	remove("test16b");

	int rc = RBFTest_16(rbfm);
	if (rc == 0) {
		cout << "Test Case 16 Passed!" << endl << endl;
	} else {
		cout << "Test Case 16 Failed!" << endl << endl;
		cout << "Variable length Record is not properly implemented." << endl;
		return -1;
	}

	return 0;
}
