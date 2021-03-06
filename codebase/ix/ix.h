#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

typedef enum
{
	leafNode=0,
	nonLeafNode=1
}nodeType;

static int leafPage;

class IndexManager {

    public:
        static IndexManager* instance();
        unsigned int flag;

        bool FileExists(const string &fileName)
       {
           struct stat stFileInfo;
           if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
           else return false;
       }

        unsigned short int rootNodeNum;


        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
        RC findMidnode(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		const void *key, const RID &rid,const unsigned int pagnum,unsigned int nextpagnum);
        RC findLeafnode(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key,
        		 int slotPosition,const unsigned int pagnum);
        RC splitEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,const void *key, const RID &rid,
        		vector<int>&parentsTree);
        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
		void printNode(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNumber) const;


        RC findPosition(IXFileHandle &ixfileHandle, AttrType attrType);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};






class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    unsigned openCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

//==================================add new variable and function====================================
    FileHandle fileHandle;
//===================================================================================================

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

	RC readRecord(const void* dataPage,const Attribute &attribute, void *dataRead,RID rid);

	RC insertRecord( void* dataPage, const Attribute &attribute,void *dataInsert,RID rid);
	RC deleteRecord( void* dataPage, const Attribute &attribute,RID rid);//maybe do not need key

};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        const void*lowKey;
        const  void*highKey;
        bool equalLow;
        bool equalHigh;
        RID IXrid_iterator;
        int pagenum;
        Attribute IX_attribute;
        IXFileHandle ix_fileHandlescaniter;
        // Terminate index scan
        RC close();
};

#endif
