#include "test_util.h"

void TEST_RM_16(const string &tableName)
{
    cout << "****In Test Case 16****" << endl;

    // Open Catalog File
    FileHandle catalog_fh;
    RC rc = rbfm->openFile(tableName, catalog_fh);
    assert(rc == success);

    // Get Catalog Attributes
    vector<Attribute> attrs;
    rc = rm->getAttributes(tableName, attrs);
    assert(rc == success);

    // print attribute name
    cout << "Catalog schema: (";
    for(unsigned i = 0; i < attrs.size(); i++)
    {
        if (i < attrs.size() - 1) cout << attrs[i].name << ", ";
        else cout << attrs[i].name << ")" << endl << endl;
    }

    RID rid;
    void *returnedData = malloc(100);

    // Set up the iterator
    RBFM_ScanIterator rsi;
    vector<string> projected_attrs;
    for (int i = 0 ; i < 3 ; i++){
      projected_attrs.push_back(attrs[i].name);
    }

    rc = rbfm->scan(catalog_fh, attrs, "", NO_OP, NULL, projected_attrs, rsi);
    assert(rc == success);

    while(rsi.getNextRecord(rid, returnedData) != RM_EOF)
    {
        rbfm->printRecord(attrs, returnedData);
    }
    rsi.close();

    free(returnedData);
    cout<<"** Test case 16 is over"<<endl;
    return;
}

int main()
{
    // NOTE: your Tables table must be called "Tables"
    string catalog_table_name = "Tables";

    cout << endl << "Test Catalog Information .." << endl;

    // Test Catalog Information
    TEST_RM_16(catalog_table_name);

    return 0;
}
