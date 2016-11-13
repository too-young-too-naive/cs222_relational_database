
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	if(PagedFileManager::instance()->FileExists(fileName)){
		cout<<"The file has already existed!"<<endl;
		return -1;
	}
	else{
		PagedFileManager::instance()->createFile(fileName);
		return 0;
	}


}

RC IndexManager::destroyFile(const string &fileName)
{
	if(PagedFileManager::instance()->FileExists(fileName)!=0){
		cout<<"No file exists!"<<endl;
		return 0;
	}
	if(PagedFileManager::instance()->destroyFile(fileName)!=0){
		cout<<"Delete file failure!"<<endl;
		return -1;
	}
    return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	if(PagedFileManager::instance()->FileExists(fileName)){
		//cerr<<"openCounter= "<<ixfileHandle.openCounter<<endl;
		if(ixfileHandle.openCounter==1) return -1;//check that whether the file has already been opened.
		if(PagedFileManager::instance()->openFile(fileName.c_str(),ixfileHandle.fileHandle)!=0) return -1;

		ixfileHandle.openCounter++;
		if(ixfileHandle.fileHandle.getNumberOfPages()==0){
			void *newPage=malloc(PAGE_SIZE);
			memset(newPage,0,PAGE_SIZE);
		    ixfileHandle.fileHandle.appendPage(newPage);
		    free(newPage);
		}
		return 0;
	}
	else{

		cout<<"No file has already existed!"<<endl;
		return -1;
	}
    //return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);
    return 0;
}

RC IndexManager::findMidnode(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,const unsigned int pagNum,unsigned int nextpagNum)
{
	int key_insert;
	memcmp(&key_insert,key,sizeof(key));
	//read root page
	char*recordMid = (char*)malloc(PAGE_SIZE);
	char*recordLeft = (char*)malloc(PAGE_SIZE);
	char*recordRight = (char*)malloc(PAGE_SIZE);//should not be page size
	int keyMid,keyLeft,keyRight;
	//ixfileHandle.fileHandle.readPage(pagNum,data);
    vector<Attribute> attrs;
    attrs.push_back(attribute);
    void*data = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pagNum,data);
    int offset = PAGE_SIZE-8;
	//read specific field to judge whether leaf
	//unsigned int flag;
	memcpy(&flag, offset + (char*) data, 4);
	RID rid_ix;
	RID rid_ixLeft;
	RID rid_ixRight;
	if(flag==1)
	{
		//not leaf
		//begin search
		//binary search
		//read all slotnum
		int a_begin,a_end, a_mid;
		int keyRight,keyLeft;
		while(a_begin>a_end)
		{
			int slotnum;
			int a_mid;
			a_mid=floor((a_begin+a_end)/2);
			//getattribute firstly
			rid_ix.pageNum = pagNum;
			rid_ix.slotNum = a_mid;
			//const void* dataPage, const Attribute &attribute,void *dataRead,RID rid
			ixfileHandle.readRecord(data,attribute,recordMid,rid_ix);//we should modify this cause we have already catch whole page in data
			memcpy(&keyMid,(char*)recordMid+4,sizeof(recordMid)-4);
			//read records a_mid
			//if attrs is varchar we need to cut record length....
			if(recordMid<key)
			{
				a_end = a_mid-1;
			}
			else
			{
				a_begin = a_mid+1; //calculate slot number
			}
			//get key of a_mid+1
			//get key of a_mid-1
			keyRight = a_mid+1;
			rid_ixRight.pageNum = pagNum;
			rid_ixRight.slotNum = keyRight;
			keyLeft  = a_mid-1;
			rid_ixLeft.pageNum = pagNum;
			rid_ixLeft.slotNum = keyLeft;
			ixfileHandle.readRecord(data,attribute,recordLeft,rid_ix);
		//	RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordLeft);
			memcpy(&keyLeft,recordLeft+4,sizeof(recordLeft)-4);
			ixfileHandle.readRecord(data,attribute,recordRight,rid_ix);
		//	RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordRight);
			memcpy(&keyRight,recordRight+4,sizeof(recordRight)-4);
			if(key_insert<=keyRight&&key_insert>keyLeft)
			{
				//find the position in this page, then go to next level;
				//read pointer-> get next page num
				memcpy(&nextpagNum,recordRight,4);
				break;
			}
		}

		free(recordMid);
		free(recordLeft);
		free(recordRight);
		free(data);
		return 0;

	}
	else
	{
		free(recordMid);
		free(recordLeft);
		free(recordRight);
		free(data);
		return 0;
	}

}

RC IndexManager::findLeafnode(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, int slotPosition,const unsigned int pagNum)
{
	int key_insert;
	memcmp(&key_insert,key,sizeof(key));
	char*recordMid = (char*)malloc(PAGE_SIZE);
	char*recordLeft = (char*)malloc(PAGE_SIZE);
	char*recordRight = (char*)malloc(PAGE_SIZE);//should not be page size
	int keyMid,keyLeft,keyRight;
	//ixfileHandle.fileHandle.readPage(pagNum,data);
    vector<Attribute> attrs;
    attrs.push_back(attribute);
    void*data = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pagNum,data);
    int offset = PAGE_SIZE-8;
	//read specific field to judge whether leaf
	//unsigned int flag;
	memcpy(&flag, offset + (char*) data, 4);
	RID rid_ix;
	RID rid_ixLeft;
	RID rid_ixRight;
	//not leaf
	//begin search
	//binary search
	//read all slotnum
	int a_begin,a_end, a_mid;
	while(a_begin>a_end)
	{
		int slotnum;
		int a_mid;
		a_mid=floor((a_begin+a_end)/2);
		//getattribute firstly
		rid_ix.pageNum = pagNum;
		rid_ix.slotNum = a_mid;
		RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordMid);//we should modify this cause we have already catch whole page in data
		memcpy(&keyMid,(char*)recordMid+4,sizeof(recordMid)-8);
		//read records a_mid
		//if attrs is varchar we need to cut record length....
		if(recordMid<key)
		{
			a_end = a_mid-1;
		}
		else
		{
			a_begin = a_mid+1; //calculate slot number
		}
		//get key of a_mid+1
		//get key of a_mid-1
		keyRight = a_mid+1;
		rid_ixRight.pageNum = pagNum;
		rid_ixRight.slotNum = keyRight;
		keyLeft  = a_mid-1;
		rid_ixLeft.pageNum = pagNum;
		rid_ixLeft.slotNum = keyLeft;
		RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordLeft);
		memcpy(&keyLeft,(char*)recordLeft+4,sizeof(recordLeft)-8);
		RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordRight);
		memcpy(&keyRight,(char*)recordRight+4,sizeof(recordRight)-8);
		if(key_insert<=keyRight&&key_insert>keyLeft)
		{
			slotPosition = keyLeft;
			//find the position in this page, then go to next level;
			//read pointer-> get next page num
			//memcpy(&rid.pageNum,(char*)recordRight+sizeof(recordRight)-8,4);
			//memcpy(&rid.slotNum,(char*)recordRight+sizeof(recordRight)-4,4);
		}
	}

	free(recordMid);
	free(recordLeft);
	free(recordRight);
	free(data);
	return 0;

}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	//short int  pageType,keySize;
	short int freeSpacePointer, slotNum;
	//short int aimNum;//the number of the entry that is in front of the insertion position
	short int recordLength,recordOffset;
	char* buffer=(char*)malloc(2000);
	//pageNum=ixfileHandle.fileHandle.getNumberOfPages();
	void* newPage=malloc(PAGE_SIZE);
	memset(newPage,0,PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(rootNodeNum,newPage);
	memcpy(&flag,(char*)newPage+PAGE_SIZE-4-4,4);//get the type of the node
	//flag =1-> midnode
	//flag =0 -> leafnode
	unsigned int pagnum = rootNodeNum;
	unsigned int nextpagnum;
	vector<int> parentTree;
	parentTree.push_back(rootNodeNum);
	int slotPosition =0;

	//combine rid with key
	int ridpag = rid.pageNum;
	int ridslot = rid.slotNum;
	void* keyPlusRid = malloc(sizeof(key)+8);//keyplusrid is entry
	memcpy(keyPlusRid, key,sizeof(key));
	memcpy((char*)keyPlusRid+sizeof(key),&ridpag,4);
	memcpy((char*)keyPlusRid+sizeof(key)+4,&ridslot,4);

	// when empty in create Bplus tree, rootpagnum = page 0; flag =0; means it is also leaf

	//find exceted position of the key should be
	while(flag==1)
	{
		//findMidnode(IXFileHandle &ixfileHandle, const Attribute &attribute,
		//const void *key, const RID &rid,const unsigned int pagNum,unsigned int nextpagNum)
		findMidnode(ixfileHandle,attribute,key,rid,pagnum,nextpagnum);
		pagnum = nextpagnum;
		parentTree.push_back(nextpagnum);
	}

	if(flag ==0)
	{
		//findLeafnode(IXFileHandle &ixfileHandle,
		//const Attribute &attribute, const void *key, RID &rid,const unsigned int pagNum)
		findLeafnode(ixfileHandle,attribute,key,slotPosition,pagnum);
		//insert keyPlusrid in PAGE: pagnum, in slot:slotPosition

	}
//get the position
	void* leafData = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pagnum,leafData);
	short int insertPosition;
	memcpy(&insertPosition,(char*)leafData+PAGE_SIZE-4-4-4*(slotPosition+1),2);
	/////////////////////move the rest records to make room for insertion/////////////////
	memcpy(&slotNum,(char*)leafData+PAGE_SIZE-4,2);
	for(int i=slotNum;i>slotPosition;i--){

		memcpy(&recordOffset,(char*)leafData+PAGE_SIZE-4-4-4*i,2);
		memcpy(&recordLength,(char*)leafData+PAGE_SIZE-4-4-4*i+2,2);
		memcpy(buffer,(char*)leafData+recordOffset,recordLength);//copy the record into buffer
		memset((char*)leafData+recordOffset,0,recordLength);//erase the original record
		memcpy((char*)leafData+recordOffset+sizeof(key),buffer,recordLength);//move the record from buffer to new place
		//not yet modify the slot to make the right order of it
	}
	////////////////////we now have made space for insertion/////////////////////////////
	memcpy((char*)leafData+insertPosition,keyPlusRid,sizeof(key)+8);//insert the entry, the entry`s size is keySize+8.

	////////////////////make the slot in order///////////////////////////////////////////
	for(int i=slotPosition+1;i<=slotNum+1;i++)
	{
		////////////////////////read the original slot information/////////////////////////////////
		memcpy(&recordOffset,leafData+PAGE_SIZE-4-4-4*i,2);
		memcpy(&recordLength,leafData+PAGE_SIZE-4-4*i+2,2);
		///////////////////////update the slot information by moving them backward one slot//////////////////////////
		memcpy(leafData+PAGE_SIZE-4-4-4*(i+1),&recordOffset,2);
		memcpy(leafData+PAGE_SIZE-4-4-4*(i+1)+2,&recordLength,2);

	}
	/////////////////////////now write the inserted entry`s slot information///////////////////////////////////////////
	short int entrySize=sizeof(key)+8;
	memcpy(leafData+PAGE_SIZE-4-4-4*(slotPosition+1),&insertPosition,2);
	memcpy(leafData+PAGE_SIZE-4-4-4*(slotPosition+1)+2,&entrySize,2);

	/////////////////////////now update the slotNum(slotNum+1) and the freeSpacePointer(freeSpacePointer+=entrySize)///////////////////////////////////////////////
	slotNum+=1;
	freeSpacePointer+=entrySize;
	memcpy(&freeSpacePointer,leafData+PAGE_SIZE-2,2);
	memcpy(&slotNum,leafData+PAGE_SIZE-4,2);

	ixfileHandle.fileHandle.writePage(pagnum,leafData);
	//ixfileHandle.fileHandle.writePage()


	free(leafData);
	free(buffer);
	free(newPage);
	free(keyPlusRid);

    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

    return -1;
}


RC IndexManager::splitEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *keyLeafMid,
		const void *key, const RID &rid,const unsigned int parents,vector<int>&parentsTree,int formerLevel)
{
	//combine key and pointer ,should in former level,cause
	//

	//should import a array of parents to show?
	//format of page
	/*[pointer2]record[pointer2]record
	 *
	 *
	 *
	 *        slot[offset[2](before pointer)+length[2](contain pointer)]; slot[offset[2](before pointer)+length[2](contain pointer)]....;FLAG[4];allslot num[2];freebyte[2];
	 *
	 */
	int midSlot,allSlot;
	int freeByte=0;
	vector<Attribute> attrs;
	attrs.push_back(attribute);
	void*data = malloc(PAGE_SIZE);
	int parents1 = parentsTree[parentsTree.size()];
	ixfileHandle.fileHandle.readPage(parents1,data);
	int offset = PAGE_SIZE-4;
	memcpy(&allSlot, offset + (char*) data, 2);
	offset = PAGE_SIZE-4+2;
	memcpy(&freeByte,offset+(char*)data,2);
	if(freeByte<(int)(2+4+sizeof(key)))
	{
		//update parents
		//from midSlot -allSlot
		//new page
		void *newPage=malloc(PAGE_SIZE);
		void *newPageForRead=malloc(PAGE_SIZE);
		memset(newPage,0,PAGE_SIZE);
		ixfileHandle.fileHandle.appendPage(newPage);
		int newPageNum=ixfileHandle.fileHandle.getNumberOfPages()-1;
		char* pointer=(char*)newPageForRead;
		char* offsetNow=(char*)newPageForRead;
		ixfileHandle.fileHandle.readPage(newPageNum,newPageForRead);
		midSlot= floor(allSlot/2);
		int j=midSlot;
		RID rid_mid;
		void* dataRead = malloc(PAGE_SIZE);//a record read..
		void* dataPageSplit = malloc(PAGE_SIZE);

		//data is the page that ..already been read
		for(;j<=allSlot;j++)
		{
			offset = PAGE_SIZE -4 -4*j;
			memcpy(&rid_mid.pageNum,offset+(char*)data,2);
			memcpy(&rid_mid.slotNum,offset+(char*)data+2,2);//pagnum slotnum which is in front?
			//read slot-record for loop
			ixfileHandle.readRecord(data,attribute,dataRead,rid_mid);
			//delete
			ixfileHandle.deleteRecord(data,attribute,rid);

			//insert
			ixfileHandle.insertRecord(dataPageSplit,attribute,dataRead,rid);//havenot insert key

		}
		ixfileHandle.fileHandle.writePage(newPageNum,dataPageSplit);
		parentsTree.erase(parentsTree.begin()+parentsTree.size()-1);//-1 or not -1?//update parents;

		return -1;
		//should split again
		//should remember splited page for spliting a page point to it //still have not add pointer. need a pointer like:
		// pointer to less KEY pointer to high
	}
	else
	{

		//insert in this parents page
	}

	RID rid_ix;
	RID rid_ixLeft;
	RID rid_ixRight;

	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    openCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}


RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount=fileHandle.readPageCounter;
    writePageCount=fileHandle.writePageCounter;
    appendPageCount=fileHandle.appendPageCounter;
    return 0;
}

RC IXFileHandle:: readRecord(const void* dataPage, const Attribute &attribute,void *dataRead,RID rid)
{
	void* newPage = malloc(PAGE_SIZE);
	memcpy(newPage,dataPage,PAGE_SIZE);
	char* pointer = (char*) newPage;
	short int recordLength, recordOffset;
	pointer = pointer + PAGE_SIZE - 4 - 4 * rid.slotNum;
	memcpy(&recordOffset, pointer, 2);
	pointer += 2;
	memcpy(&recordLength, pointer, 2);//length should contain 4 pointer bytes

	if(recordLength==-1)
	{
		return -1;
	}
	memcpy(dataRead, (char*) newPage + recordOffset, recordLength);
	free(newPage);
	return 0;
}

RC IXFileHandle:: insertRecord( void* dataPage, const Attribute &attribute,void *dataInsert,RID rid)//maybe do not need key
{
	//dataInsert should already contain pointer
	//cout<<"create a new page"<<endl;
	short int freeSpacePointer, slotNum;
	short int recordOffset, recordLength;
	int dataSize=sizeof(dataInsert);
	void *newPage=malloc(PAGE_SIZE);
	memset(newPage,0,PAGE_SIZE);
	int newPageNum=fileHandle.getNumberOfPages()-1;
	char* pointer=(char*)dataPage;
	char* offsetNow=(char*)dataPage;
	pointer=pointer+PAGE_SIZE-2;
	memcpy(&freeSpacePointer,pointer,2);
	pointer-=2;
	memcpy(&slotNum,pointer,2);
	slotNum+=1;
	offsetNow+=freeSpacePointer;
	RecordBasedFileManager::instance()->writeRecord(dataInsert,dataSize,offsetNow,slotNum,newPageNum,rid);
	recordLength=dataSize;
	recordOffset=freeSpacePointer;
	freeSpacePointer+=dataSize;
	memcpy(pointer,&slotNum,2);
	pointer+=2;
	memcpy(pointer,&freeSpacePointer,2);
	pointer=pointer-2-4*rid.slotNum;
	memcpy(pointer,&recordOffset,2);
	pointer+=2;
	memcpy(pointer,&recordLength,2);
	return 0;
}


RC IXFileHandle:: deleteRecord( void* dataPage, const Attribute &attribute,RID rid)//maybe do not need key
{

	//delete in this function should ->slot point to next record rather than keep as -1; this function not finished!
	//different way to move left records
	short int freeSpacePointer, slotNum;
	short int recordLength, recordOffset;
 	void *newPage=malloc(PAGE_SIZE);
	memset(newPage,0,PAGE_SIZE);
	char* pointer=(char*)newPage;
	fileHandle.readPage(rid.pageNum,newPage);
	pointer=pointer+PAGE_SIZE-2;
	memcpy(&freeSpacePointer,pointer,2);
	pointer-=2;
	memcpy(&slotNum,pointer,2);
	pointer=pointer-4*rid.slotNum;
	memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4*rid.slotNum,2);
	memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4-4*rid.slotNum+2,2);
	short int originalOffset=recordOffset;
	short int originalLength=recordLength;

	memset((char*)newPage+originalOffset,0,originalLength);
	short int deletedLength=-1;///////////////////////////////deleted record`s slot`s recordLength is -1!!!!!!!!!!!!!!!!!!!!!!!!!
	memcpy((char*)newPage+PAGE_SIZE-4-4*rid.slotNum+2,&deletedLength,2);
	//ä¹åçrecordsç¼©è¿
    char* buffer=(char*)malloc(4000);

//====================================================================================================
    for(int i=rid.slotNum+1;i<=slotNum;i++)
    {

    	memset(buffer,0,4000);
    	//short int newRecordOffset;
    	memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4*i,2);
    	memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4-4*i+2,2);

    	if(recordLength==-1)//the deleted record and updated-in-original-page record, there original positions will be ignored
    	{
    		continue;//////////////////////////this slot`s record has been deleted, just go on to next loop
    	}

    	short int newRecordOffset=recordOffset-originalLength;//è®¡ç®åé¢çrecordçoffsetè¯¥åæå¤å°
        	//ç¼©è¿è¢«å é¤recordåçæ¯ä¸æ¡record

    	memcpy((char*)buffer,recordOffset+(char*)newPage,recordLength);
        memset((char*)newPage+recordOffset,0,recordLength);
        memcpy((char*)newPage+newRecordOffset,buffer,recordLength);
    	memcpy((char*)newPage+PAGE_SIZE-4-4*i,&newRecordOffset,2);//æ´æ°æ¯ä¸ä¸ªrecordçå­å¨slotéé¢çrecord offset,don`t need to update the slotNum
    }
//=======================================================================================================================================
	freeSpacePointer=freeSpacePointer-originalLength;
	memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);
    free(buffer);
    free(newPage);
    return 0;

}
