#include "test_util.h"

int main()
{
    // Remove files that might be created by previous test run
	if (FileExists("tbl_employee")){
		deleteTable("tbl_employee");
	}
	if (FileExists("tbl_employee2")){
		deleteTable("tbl_employee2");
	}
	if (FileExists("tbl_employee3")){
		deleteTable("tbl_employee3");
	}
	if (FileExists("tbl_employee4")){
		deleteTable("tbl_employee4");
	}

	// Basic Functions
  cout << endl << "Create Tables ..." << endl;

  // Create Table tbl_employee
  createTable("tbl_employee");

  // Create Table tbl_employee2
	createTable("tbl_employee2");

  // Create Table tbl_employee3
  createTable("tbl_employee3");

  // Create Table tbl_employee4
  createLargeTable("tbl_employee4");

  return 0;
}
