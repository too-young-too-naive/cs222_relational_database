
#ifndef _rm_h_
#define _rm_h_

#include <stdio.h>
#include <string>
#include <vector>
#include <iostream>
#include <iterator>
#include <fstream>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"
using namespace std;

# define RM_EOF (-1)  // end of a scan operator


// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
	  RM_ScanIterator(){};
	  ~RM_ScanIterator(){};

	  // "data" follows the same format as RelationManager::insertTuple()
	  RC getNextTuple(RID &rid, void *data);
	  RC close();

	  FileHandle fileHandlescaniterRM;
	  RID rid_iteratorRM;
	  vector<Attribute> recordDescriptor_iteratorRM;
	  CompOp compOpRM;
	  const void *valueRM;
	  int pagenum;
	  string  conditionAttrbutestr;
	  vector<string> attributeNames_iteratorRM;
	  RBFM_ScanIterator rbfm_iterator;
};

class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator();  	// Constructor
  ~RM_IndexScanIterator(); 	// Destructor

  IX_ScanIterator ix_scanIterator;
  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close();             			// Terminate index scan
};


typedef struct{
	int tableid;
//	char tablename[50] ="#################################################";//contain /0
	char tablename[50] ="                                                 ";
	char filename[50] = "                                                 ";
} Tables;

typedef struct {
	int tableid;
	char columnname[50] = "                                                 ";
	//string columnname;
	int columntype;
	int columnlength;
	int columnposition;
} Columns;

typedef struct
{
 int slot_all_num;    //number of existed slot
 int left;    // free bytes  in the page
} page_info;


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();
  vector<Tables> table;
  vector<Columns> column;

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

    RC destroyIndex(const string &tableName, const string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const string &tableName,
                          const string &attributeName,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          RM_IndexScanIterator &rm_IndexScanIterator
         );

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
};

#endif
