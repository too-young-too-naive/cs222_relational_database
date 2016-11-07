#include "rbfm.h"
#include <string>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {

   if(FileExists(fileName.c_str())){
	   cout<<"File has already existed!"<<endl;
	   return -1;
   }
   else{
	 _pf_manager->createFile(fileName.c_str());

	  return 0;
	 }
}



RC RecordBasedFileManager::destroyFile(const string &fileName) {
    _pf_manager->destroyFile(fileName);
    return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
   if(FileExists(fileName)){

   _pf_manager->openFile(fileName.c_str(),fileHandle);
   if(fileHandle.getNumberOfPages()==0){
		void *newPage=malloc(PAGE_SIZE);

		memset(newPage,0,PAGE_SIZE);
		fileHandle.appendPage(newPage);
		free(newPage);

   }

   int n=fileHandle.getNumberOfPages();


 //  cout<<"from openfile, the page of number is: "<<n<<endl;

   return 0;


   }
   else{
//	   cout<<"No file has already existed!"<<endl;
	   return -1;
   }

}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {

	//PagedFileManager * pfm = PagedFileManager::instance();
    //pfm->closeFile(fileHandle);
   // return 0;
	_pf_manager->closeFile(fileHandle);
	return 0;

}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
	int n=fileHandle.getNumberOfPages();
//	cout<<"the page number is "<<n<<endl;
	int dataSize;
	dataSize=calculateFieldsSize(recordDescriptor,data);
//	cout<<"data size is "<<dataSize<<endl;
	//cout<<"Data size is "<<dataSize<<endl;
	int pageNumber=fileHandle.getNumberOfPages()-1;
	if((pageNumber==fileHandle.getNumberOfPages()-1)&&(pageNumber>=0))
	{   //cout<<"page number is (start from 0) "<<pageNumber<<endl;
		short int freeSpacePointer, slotNum;
		short int recordLength, recordOffset;
   		void *newPage=malloc(PAGE_SIZE);
   		char* pointer=(char*)newPage;
   		char* offsetNow=(char*)newPage;
   		fileHandle.readPage(pageNumber,newPage);
   		pointer=pointer+PAGE_SIZE-2;
   		memcpy(&freeSpacePointer,pointer,2);
   		pointer-=2;
   		memcpy(&slotNum,pointer,2);
   		//cout<<" before inserting record free space pointer is "<<freeSpacePointer<<endl;
		//cout<<"before inserting record ,slot num is "<<slotNum<<endl;
   	    short int freeSpace=PAGE_SIZE-freeSpacePointer-4-4*slotNum-4;//4:freespace and slotNum, 4:new added slot
		//cout<<"before insert free space is "<<freeSpace<<endl;
   	    if(freeSpace>=dataSize)
   	    {
			slotNum+=1;
			offsetNow+=freeSpacePointer;
			//cout<<"pageNumber is "<<pageNumber<<endl;
	   		writeRecord(data,dataSize,offsetNow,slotNum,pageNumber,rid);
	   		recordOffset=freeSpacePointer;
	   		memcpy(pointer,&slotNum,2);
	   		pointer+=2;
	   		freeSpacePointer=freeSpacePointer+dataSize;
	   		memcpy(pointer,&freeSpacePointer,2);
	   		pointer=pointer-2-slotNum*4;
	   		memcpy(pointer,&recordOffset,2);
	   		pointer+=2;
	   		recordLength=dataSize;
	   		memcpy(pointer,&recordLength,2);
	   		fileHandle.writePage(pageNumber,newPage);
			free(newPage);
			return 0;

   	    }
   	    else{
   	    	fileHandle.writePage(pageNumber,newPage);
   	    	goto RECURRSIVE;
   	    }
	}
	RECURRSIVE:
	{

	   for(int i=0;i<fileHandle.getNumberOfPages();i++)
	  {

		        short int freeSpacePointer, slotNum;
		        short int recordOffset, recordLength;
		   		void *newPage=malloc(PAGE_SIZE);
		   		char* pointer=(char*)newPage;
		   		char* offsetNow=(char*)newPage;
		   		fileHandle.readPage(i,newPage);
		   		pointer=pointer+PAGE_SIZE-2;
		   		memcpy(&freeSpacePointer,pointer,2);
		   		pointer-=2;
		   		memcpy(&slotNum,pointer,2);
		   	    int freeSpace=PAGE_SIZE-freeSpacePointer-4-4*slotNum;
		   	    if(freeSpace>=dataSize)
		   	    {
		   	    	offsetNow+=freeSpacePointer;
		   	    	writeRecord(data,dataSize,offsetNow,slotNum+1,i,rid);
		   	        recordOffset=freeSpacePointer;
		   	    	slotNum+=1;
		 	   		memcpy(pointer,&slotNum,2);
		 	   		pointer+=2;
		  	   		freeSpacePointer=freeSpacePointer+dataSize;
		  	   		memcpy(pointer,&freeSpacePointer,2);
		  	   		pointer=pointer-2-slotNum*4;
		   	    	memcpy(pointer,&recordOffset,2);
			   		pointer+=2;
			   		recordLength=dataSize;
		   	   		memcpy(pointer,&recordLength,2);
		   	   		fileHandle.writePage(i,newPage);
		   	   		free(newPage);
		   	   		return 0;
		   	   		break;


		   	    }
		   	    else
		   	    {
		   	    	free(newPage);
		   	    	if(i=fileHandle.getNumberOfPages()-1)
		   	    	{
		   	    		break;
		   	    	}
		   	    	else
		   	    	{
		   	    		continue;
		   	    	}

		   	    }
	  }
		   	        //cout<<"create a new page"<<endl;
		 		    short int freeSpacePointer, slotNum;
		 		    short int recordOffset, recordLength;
		 		   	void *newPage=malloc(PAGE_SIZE);
		 		   	memset(newPage,0,PAGE_SIZE);
		 		   	fileHandle.appendPage(newPage);
		   	    	int newPageNum=fileHandle.getNumberOfPages()-1;
		 		   	char* pointer=(char*)newPage;
		 		   	char* offsetNow=(char*)newPage;
		 		   	fileHandle.readPage(newPageNum,newPage);
		 		   	pointer=pointer+PAGE_SIZE-2;
		 		   	memcpy(&freeSpacePointer,pointer,2);
		 		   	pointer-=2;
		 		   	memcpy(&slotNum,pointer,2);
		 		   	slotNum+=1;
                    offsetNow+=freeSpacePointer;
		 		   	writeRecord(data,dataSize,offsetNow,slotNum,newPageNum,rid);
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
		 	        fileHandle.writePage(rid.pageNum,newPage);
		 	    	free(newPage);
//		 	    	cout<<"out record"<<endl;
		 	    	return 0;




}
}



RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
//	cout<<"recordDescriptoraa"<<recordDescriptor.size()<<endl;
	void* buffer = malloc(PAGE_SIZE);
	void* newPage = malloc(PAGE_SIZE);
	char* pointer = (char*) newPage;
//	cout<<rid.slotNum<<"ridslotNUM"<<endl;
//	cout<<rid.pageNum<<"ridpageNUM"<<endl;
	fileHandle.readPage(rid.pageNum, newPage);

	//test
//	char* newPagea = (char*)malloc(PAGE_SIZE);
//	memcpy(newPagea, (char*)newPage+9,50);
//	cout<<"newpagesss char:"<<newPagea<<endl;
	//test end

//	int a =0;
//	memcpy(&a,(char*)newPage+5,4);
//	cout<<"aaaaaaaaaa"<<a<<endl;


	short int recordLength, recordOffset;
	pointer = pointer + PAGE_SIZE - 4 - 4 * rid.slotNum;
	memcpy(&recordOffset, pointer, 2);
	pointer += 2;
	memcpy(&recordLength, pointer, 2);

	if(recordLength==-1)
	{
//		cout<<"this record has been deleted! No tuple is read!"<<endl;
	//	memset(data,0,4000);
		return -1;

	}
	int nullIndicator = ceil((double) recordDescriptor.size() / 8.0);
	char* nullDirectory = (char*) malloc(nullIndicator);
	memcpy(nullDirectory, (char*) newPage + recordOffset, nullIndicator);
	int nullBitNum;
	int nullIndicatorByte;
	memcpy(buffer, (char*) newPage + recordOffset, recordLength);
	//memcpy(data, buffer, nullIndicator);   //write the NULL indicator into data.
	int offsetNow = ceil((double) recordDescriptor.size() / 8.0);

	int offset = ceil((double) recordDescriptor.size() / 8.0);
	//cout<<"this record has been deleted!"<<endl;
	if (recordLength == 4)
	{

		//it is a tombstone

		TOMBSTONE transferIndicator;
		memcpy(&transferIndicator.transfer_pageNum,recordOffset + (char*) newPage, 2);
		memcpy(&transferIndicator.transfer_slotNum,recordOffset + (char*) newPage+2, 2);



		void* transferPage = malloc(PAGE_SIZE);
		fileHandle.readPage(transferIndicator.transfer_pageNum, transferPage);//read the transfer page
		memcpy(&recordOffset,(char*)transferPage+PAGE_SIZE-4-transferIndicator.transfer_slotNum*4,2);
		memcpy(&recordLength,(char*)transferPage+PAGE_SIZE-4-transferIndicator.transfer_slotNum*4+2,2);
		memcpy(data,(char*)transferPage+recordOffset,recordLength);//read the record into data
		//fileHandle.writePage(transferIndicator.transfer_pageNum, transferPage);
		/////////////////////////////////
	//	printRecord(recordDescriptor,data);

		free(transferPage);
		free(newPage);
        free(nullDirectory);
        free(buffer);
		return 0;

	}

     else
	{
    	memcpy(data, buffer, nullIndicator);   //write the NULL indicator into data.

   // 	cout<<recordDescriptor.size()<<"recordDescriptor.size()"<<endl;
		for (unsigned int i = 0; i < recordDescriptor.size(); i++)
		{
			nullIndicatorByte = i / 8;
			nullBitNum = i % 8;
			if (nullDirectory[nullIndicatorByte] & (1 << 7 - nullBitNum))
			{

				continue;
				//cout<<recordDescriptor[i].name<<": (NULL)"<< NULL <<endl;
			}

			else
			{
		//		cout<<":::::::::::::::::recordDescriptor[i].type"<<recordDescriptor[i].type<<endl;

				switch (recordDescriptor[i].type)
				{
				case TypeInt:
				{
					int intField;
					memcpy(&intField, offset + (char*) buffer, 4);
					//cout<<recordDescriptor[i].name<<":"<<intField<<endl;
					offset += 4;
					memcpy((char*) data + offsetNow, &intField, 4);
					offsetNow += 4;
					//fieldLength += recordDescriptor[i].length;
					break;
				}
				case TypeReal:
				{
					float realField;
					memcpy(&realField, offset + (char*) buffer, 4);
					offset += 4;
					memcpy((char*) data + offsetNow, &realField, 4); //write the real type field into data.
					offsetNow += 4;
					//cout<<recordDescriptor[i].name<<":"<<realField<<endl;
					break;
				}
				case TypeVarChar:
				{
		//			cout<<"enter"<<endl;
					int varcharFieldLength;
					char* varcharField;
					memcpy(&varcharFieldLength, (char*) buffer + offset, 4);
//					cout<<"varcharFieldLength"<<varcharFieldLength<<endl;
					memcpy((char*) data + offsetNow, (char*) buffer + offset,4); //write the length information of varchar filed into data.
					offset += 4;
					varcharField = (char*) malloc(varcharFieldLength);
					memcpy(varcharField, offset + (char*) buffer,varcharFieldLength);
//					cout<<recordDescriptor[i].name<<":"<<varcharField<<endl;
					offset += varcharFieldLength;
					offsetNow += 4;
					memcpy((char*) data + offsetNow, varcharField,
							varcharFieldLength); //write the text information of varchar field into data.
					offsetNow += varcharFieldLength;
					free(varcharField);
					break;
				}
				}
			}

		}
	//	printRecord(recordDescriptor,data);

		free(newPage);
		free(buffer);
		free(nullDirectory);
//		cout<<"end read"<<endl;
		return 0;
	}
}




RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {

	 int offset=0;
	 int nullBit=ceil((double)recordDescriptor.size()/8.0);
	 offset=offset+nullBit;
	 char* nullIndicator=(char*) malloc(nullBit);
	 memcpy(nullIndicator,data,nullBit);
	 int nullBitNum;
	 int nullIndicatorByte;
	// memcpy(&nullIndicator,(char*)data+nullBit,nullBit);
	// cout<<nullIndicator<<endl;
		for(unsigned int i=0;i<recordDescriptor.size();i++){
            nullIndicatorByte=i/8;
			nullBitNum=i%8;
			if(nullIndicator[nullIndicatorByte]&(1<<7-nullBitNum)){
				cout<<recordDescriptor[i].name<<": (NULL)"<< NULL <<endl;
			}
			else
			{



			switch(recordDescriptor[i].type){

			case TypeReal:{
				float realField;
				memcpy(&realField,offset+(char*)data,4);
				offset+=4;
				cout<<recordDescriptor[i].name<<": (TypeReal)"<<realField<<endl;
				break;
			}

			case TypeInt:{
				int intField;
				memcpy(&intField,offset+(char*)data,4);
	            cout<<recordDescriptor[i].name<<": (TypeInt)"<<intField<<endl;
				offset+= 4;
				//fieldLength += recordDescriptor[i].length;
		     	break;
			}

			case TypeVarChar:{
				int varcharFieldLength;
				char* varcharField;
				memcpy(&varcharFieldLength,(char*)data+offset,4);
				offset+=4;
				varcharField=(char*)malloc(varcharFieldLength);
				memcpy(varcharField,offset+(char*)data,varcharFieldLength);
				cout<<recordDescriptor[i].name<<": (TypeVarChar)"<<varcharField<<endl;
				offset+=varcharFieldLength;
				free(varcharField);
				break;
			}
			}

			}
		}

		return 0;
}



RC RecordBasedFileManager::calculateFieldsSize(const vector<Attribute> recordDescriptor, const void*data){
	int offset=ceil((double)recordDescriptor.size()/8.0);
//	cout<<"offset end"<<offset<<endl;
		for(unsigned int i=0;i<recordDescriptor.size();i++){
//			cout<<"recordDescriptor[i].type"<<recordDescriptor[i].type<<endl;
//			cout<<"iiiiiiiiiii"<<i<<endl;
				switch(recordDescriptor[i].type){
				case TypeInt:{
					offset+= 4;
//					cout<<"enter int"<<endl;
					break;
				}
				case TypeReal:{
					offset+=4;
//					cout<<"enter real"<<endl;
					break;
				}
				case TypeVarChar:{
					int *var=(int*)malloc(1000);
					memcpy(var,(char*)data+offset,4);
					offset+=4;
					char*aa=(char*)malloc(50);
					memcpy(aa,(char*)data+offset,50);
//					cout<<"var  "<<*var<<endl;
//					cout<<"char"<<aa<<endl;
					offset+=*var;
					free(var);
					break;
				}
				}
	//			cout<<"offset []"<<i<<"type"<<offset<<endl;
		}
//		cout<<"offset end"<<offset<<endl;
			return offset;
}


RC RecordBasedFileManager::writeRecord(const void* data,int dataSize,char* pointer,int slotNum,int pageNum,RID &rid)
{
	rid.pageNum=pageNum;
	rid.slotNum=slotNum;
	memcpy(pointer,data,dataSize);
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	short int freeSpacePointer, slotNum;
	short int recordLength, recordOffset;
 	void *newPage=malloc(PAGE_SIZE);
	memset(newPage,0,PAGE_SIZE);
	char* pointer=(char*)newPage;
	fileHandle.readPage(rid.pageNum,newPage);//å°æ­¤é¡µè¯»å¥åå­
	pointer=pointer+PAGE_SIZE-2;
	memcpy(&freeSpacePointer,pointer,2);
	pointer-=2;
	memcpy(&slotNum,pointer,2);

	//å¯»æ¾ç®æ recordçä½ç½®ï¼ç¡®å®lengthåoffset
	pointer=pointer-4*rid.slotNum;
	memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4*rid.slotNum,2);
	memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4-4*rid.slotNum+2,2);
	short int originalOffset=recordOffset;
	short int originalLength=recordLength;
//	cout<<"originalLength is "<<originalLength<<endl;
//	cout<<"originalOffset is "<<originalOffset<<endl;

	//check that whether this record`s slot`s recordLength is -1, make sure whether this record has been deleted.
	if(recordLength==-1)
	{
		cout<<"This record has been already deleted, delete failure!"<<endl;
		return -1;
	}


	//originalLength=recordLength;
	//originalOffset=recordOffset;


	/////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////ç¡®å®æ­¤recordæ¯å¦æ¯ä¸ä¸ªtombstone//////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	if (originalLength ==4)//make sure it is a tombstone  // change length to -1
	{
		//delete transferRecord
		TOMBSTONE transferIndicator;
		memcpy(&transferIndicator.transfer_pageNum,(char*)newPage+originalOffset,2);
		memcpy(&transferIndicator.transfer_slotNum,(char*)newPage+originalOffset+2,2);
	//	cout<<"transferIndicator.transfer_pageNum is : "<<transferIndicator.transfer_pageNum<<endl;
	//	cout<<"transferIndicator.transfer_slotNum is : "<<transferIndicator.transfer_slotNum<<endl;






        if(transferIndicator.transfer_pageNum!=rid.pageNum){
   //        cout<<"-----------------------------------------------------------------------"<<endl;
		   deleteTransferRecord(fileHandle,transferIndicator);//use the deleteTransferRecord() function
	//	   cout<<"----------------------------------------------------------------------------"<<endl;
        }
        else{
        	short int localRecordOffset, localRecordLength;
        	memcpy(&localRecordOffset,(char*)newPage+PAGE_SIZE-4-transferIndicator.transfer_slotNum*4,2);
        	memcpy(&localRecordLength,(char*)newPage+PAGE_SIZE-4-transferIndicator.transfer_slotNum*4+2,2);
        	memset((char*)newPage+localRecordOffset,0,localRecordLength);//delete the transfered record
   //    	cout<<"delete the local transfer record offset is "<<localRecordOffset<<endl;
   //     	cout<<"delete the local transfer record length is "<<localRecordLength<<endl;
        	short int localDeletedLength=-1;
        	memcpy((char*)newPage+PAGE_SIZE-4-4*transferIndicator.transfer_slotNum+2,&localDeletedLength,2);
        	char* localBuffer=(char*)malloc(4000);
            for(int j=transferIndicator.transfer_slotNum+1;j<=slotNum;j++)
            {

            	memset(localBuffer,0,4000);
            	//short int newRecordOffset;
            	memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4*j,2);
            	memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4-4*j+2,2);

            	if(recordLength==-1)//the deleted record and updated-in-original-page record, there original positions will be ignored
            	{
            		continue;//////////////////////////this slot`s record has been deleted, just go on to next loop
            	}

            	short int newLocalRecordOffset=recordOffset-localRecordLength;//è®¡ç®åé¢çrecordçoffsetè¯¥åæå¤å°
                	//ç¼©è¿è¢«å é¤recordåçæ¯ä¸æ¡record

            	memcpy((char*)localBuffer,recordOffset+(char*)newPage,recordLength);
                memset((char*)newPage+recordOffset,0,recordLength);
                memcpy((char*)newPage+newLocalRecordOffset,localBuffer,recordLength);
            	memcpy((char*)newPage+PAGE_SIZE-4-4*j,&newLocalRecordOffset,2);//æ´æ°æ¯ä¸ä¸ªrecordçå­å¨slotéé¢çrecord offset,don`t need to update the slotNum
            }
            freeSpacePointer-=localRecordLength;
            free(localBuffer);



        }



	}



	memset((char*)newPage+originalOffset,0,originalLength);
	//å é¤recordårecord lengthç½®ä¸º-1
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
	memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);//æ´æ°free space pointer
    fileHandle.writePage(rid.pageNum, newPage);//å°æ­¤é¡µåå¥disk
    free(buffer);
    free(newPage);
  //  cout<<"================================================================================================="<<endl;
	return 0;

}




RC RecordBasedFileManager::deleteTransferRecord(FileHandle &fileHandle, TOMBSTONE &transferIndicator)
{
	//i=1;
	short int freeSpacePointer,slotNum;
	short int recordOffset;
	short int recordLength;
	short int originalOffset, originalLength;
	void* transferPage=malloc(PAGE_SIZE);
	memset(transferPage,0,PAGE_SIZE);
	fileHandle.readPage(transferIndicator.transfer_pageNum, transferPage);
	memcpy(&freeSpacePointer,(char*)transferPage+PAGE_SIZE-2,2);// acquire the free space pointer
	memcpy(&slotNum,(char*)transferPage+PAGE_SIZE-4,2);//acquire the slot number of this page
	memcpy(&originalOffset,(char*)transferPage+PAGE_SIZE-4-4*transferIndicator.transfer_slotNum,2);
	memcpy(&originalLength,(char*)transferPage+PAGE_SIZE-4-4*transferIndicator.transfer_slotNum+2,2);

	if(originalLength==-1) {
	//	cout<<"This transfer record has been deleted, delete failure"<<endl;
		return -1;
	}

	memset((char*)transferPage+originalOffset,0,originalLength);//delete the transfer record



	short int deletedLength=-1;
	memcpy((char*)transferPage+PAGE_SIZE-4-4*(transferIndicator.transfer_slotNum)+2,&deletedLength,2);//make the slot`s length -1 which means this record has been deleted.
	//freeSpacePointer-=transferIndicator.transfer_recordLength;
//	memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);//store the new free space pointer

//-------------------------------------è¿ä¸ªç§»ä½æ¯ç®åä¸ºæ­¢æå®å¤çï¼å¶ä»çé½åèè¿ä¸ª------------------------------------------------
	/////////////////////////å¦ä½ç§»ä½ç¼©è¿åé¢çrecordï¼ï¼ï¼ï¼
	//now we have added slot when updating a new record in another page;
	//and we can shrink records now
	char* buffer=(char*)malloc(4000);

//	cout<<"delete transfer record`s origianl lengthe is "<<originalLength<<endl;

//	cout<<"delete transfer record`a transfer pagenum is "<<transferIndicator.transfer_pageNum<<endl;
//	cout<<"delete transfer record`s transfer slotnum is "<<transferIndicator.transfer_slotNum<<endl;

	for(int i=transferIndicator.transfer_slotNum+1;i<=slotNum;i++)
	{

		   memset(buffer,0,4000);
		   memcpy(&recordOffset,(char*)transferPage+PAGE_SIZE-4-4*i,2);//æ±åºç®æ recordåä¸æ¡recordçoffset
		   memcpy(&recordLength,(char*)transferPage+PAGE_SIZE-4-4*i+2,2);//æ±åºrecord åä¸æ¡recordçlength

		   //check the slot`s length whether is -1, if yes, just skip it and go on to the next loop.
		   if(recordLength==-1){

			   continue;
		   }

		   short int newRecordOffset=recordOffset-originalLength;

		   memcpy((char*)buffer,(char*)transferPage+recordOffset,recordLength);//åå¤å¶recordå°bufferä¸ï¼
		   memset((char*)transferPage+recordOffset,0,recordLength);//årecordç©ºé´ç©ºé´æ¸é¶ï¼é²æ­¢dataéå 
		   memcpy((char*)transferPage+newRecordOffset,(char*)buffer,recordLength);//å°bufferéé¢çrecordå¤å¶å°è¯¥ç§»ä½å°çä½ç½®
		   memcpy((char*)transferPage+PAGE_SIZE-4-4*i,&newRecordOffset,2);//write the new offset into slot

	}
	   freeSpacePointer-=originalLength;
	   memcpy((char*)transferPage+PAGE_SIZE-2,&freeSpacePointer,2);//store the new free space pointer
	   fileHandle.writePage(transferIndicator.transfer_pageNum,transferPage);
	   free(buffer);
	   free(transferPage);
	   return 0;
//----------------------------------------------------------------------------------------------------------------

}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{



	short int dataSize;
	dataSize=calculateFieldsSize(recordDescriptor, data);


	short int freeSpacePointer, slotNum;
	short int recordLength, recordOffset;
	void *newPage=malloc(PAGE_SIZE);
	void *buffer=malloc(PAGE_SIZE);
	memset(newPage,0,PAGE_SIZE);
	char* pointer=(char*)newPage;
	fileHandle.readPage(rid.pageNum,newPage);//将此页读入内存
	pointer=pointer+PAGE_SIZE-2;
	memcpy(&freeSpacePointer,pointer,2);
	pointer-=2;
	memcpy(&slotNum,pointer,2);
	memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4*rid.slotNum,2);
	memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4+2-4*rid.slotNum,2);
	int originalLength=recordLength;
	int originalOffset=recordOffset;
	//=================================== update updated records---start =======================================================
	if(originalLength==4){

		//This is a tombstone we need to update its record which is transfered into another page.
		TOMBSTONE transferIndicator;
        memcpy(&transferIndicator.transfer_pageNum,(char*)newPage+originalOffset,2);
        memcpy(&transferIndicator.transfer_slotNum,(char*)newPage+originalOffset+2,2);

		transferIndicator=updateTransferdRecord(fileHandle, data,  dataSize,transferIndicator,rid);//updateTransferdRecord() returns a struct

		//if transferIndicator.pageNum ==rid.PageNum, in the function--updateTransferdRecord(),the page has already write
		//back into disk, we need to read it into memory again!
		if(transferIndicator.transfer_pageNum==rid.pageNum) fileHandle.readPage(transferIndicator.transfer_pageNum,newPage);

		//change the value of tombstone.
		memcpy((char*)newPage+originalOffset,&transferIndicator.transfer_pageNum,2);
		memcpy((char*)newPage+originalOffset+2,&transferIndicator.transfer_slotNum,2);
		fileHandle.writePage(transferIndicator.transfer_pageNum,newPage);//write back into disk
		free(newPage);
		free(buffer);
		return 0;

        }


	//===================================== update updated records---ends =============================================================

	if(originalLength>=dataSize)
	{
		//the space enough to update

		memset((char*)newPage+originalOffset,0,originalLength);//擦除原record
		memcpy((char*)newPage+originalOffset,data,dataSize);//写上新的record
		memcpy((char*)newPage+PAGE_SIZE-4-4*rid.slotNum+2,&dataSize,2);//更新slot中的record length
		short int difference=originalLength-dataSize;
//=====================================================================================================

		for(int i=rid.slotNum+1;i<=slotNum;i++)
		{

		   memset(buffer,0,4000);
		   memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4*i,2);//求出目标record后一条record的offset
		   memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4-4*i+2,2);//求出record 后一条record的length
		  //check whether the record has been deleted.
		   if(recordLength==-1){
			   continue;
		   }
		   int newRecordOffset=recordOffset-difference;
		  // memset((char*)newPage+newRecordOffset,0,recordLength);//给下一条record移位前空间清零
		   memcpy(buffer,(char*)newPage+recordOffset,recordLength);//将record复制到buffer上
		   memset((char*)newPage+recordOffset,0,recordLength);//清空原位置的record
		   memcpy((char*)newPage+newRecordOffset,buffer,recordLength);//移位
		   memcpy((char*)newPage+PAGE_SIZE-4-4*i,&newRecordOffset,2);//write the new offset into slot
		}
//==================================================================================================================


	   freeSpacePointer-=difference;
	   memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);//update the free space pointer;

		free(buffer);
		fileHandle.writePage(rid.pageNum, newPage);
		free(newPage);
		return 0;

	}
	else//更新后length大于原来，需要另找空间
	{

		TOMBSTONE transferIndicator;//define a tombstone

		//leave a tombstone
		//先缩进再比较本页freespace是否足够塞入new record
		int freeSpace=PAGE_SIZE-freeSpacePointer-4-4*slotNum+originalLength-4-4;//4 is the new added slot`s size, another 4 is the tombstone`s size!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		if(freeSpace>=dataSize)
		{
			transferIndicator.transfer_pageNum=rid.pageNum;
			transferIndicator.transfer_slotNum=slotNum+1;

			if (originalLength > 0) memset((char*)newPage+originalOffset,0,originalLength);//擦除原record

			memcpy((char*)newPage+originalOffset,&transferIndicator.transfer_pageNum,2);
			memcpy((char*)newPage+originalOffset+2,&transferIndicator.transfer_slotNum,2);

			short int tombstoneLength=4;
			short int difference = originalLength - 4;

//=====================================================================================================
			// this page has enough space,  need to create a tombstone
			for(int i=rid.slotNum+1;i<=slotNum;i++)
			{
				memset(buffer,0,4000);
			   memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4*i,2);//求出目标record后一条record的offset
			   memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4-4*i+2,2);//求出record 后一条record的length

			   if(recordLength==-1){

				   continue;

			   }

			 //  void* buffer=malloc(recordLength);//
			   int newRecordOffset=recordOffset-difference;
			   memcpy(buffer,(char*)newPage+recordOffset,recordLength);
			   memset((char*)newPage+recordOffset,0,recordLength);// record移位前空间清零

			   memcpy((char*)newPage+newRecordOffset,buffer,recordLength);//移位

			   memcpy((char*)newPage+PAGE_SIZE-4-4*i,&newRecordOffset,2);//update the new offset into slot


			}
//=================================================================================================================

			freeSpacePointer=freeSpacePointer-difference;
			memcpy((char*)newPage+freeSpacePointer,data,dataSize);//缩进后再free space pointer处塞入new record
            //add a new slot
			slotNum+=1;
            memcpy((char*)newPage+PAGE_SIZE-4-4*slotNum,&freeSpacePointer,2);
            memcpy((char*)newPage+PAGE_SIZE-4-4*slotNum+2,&dataSize,2);

            //update original slot`s information
			//recordOffset=freeSpacePointer;//////////////////

			//recordLength=4;
			//memcpy((char*)newPage+PAGE_SIZE-4-4*rid.slotNum,&recordOffset,2);//update original record`s slot`s offset
			memcpy((char*)newPage+PAGE_SIZE-4-4*rid.slotNum+2,&tombstoneLength,2);//update original record`s slot`s length

			//update free space pointer`s and slotNum`s information
			freeSpacePointer+=dataSize;
			memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);//update free space pointer
			memcpy((char*)newPage+PAGE_SIZE-4,&slotNum,2);


		    fileHandle.writePage(rid.pageNum,newPage);

			free(buffer);

		    free(newPage);

		    return 0;




		}

		else//本页没有足够空间更新了，到另一页写个新的record
		{


			//TOMBSTONE transferIndicator;
			if (originalLength > 0) memset((char*)newPage+originalOffset,0,originalLength);//擦除原record

			//transfer the new record to another page
//////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////
			transferRecord(fileHandle, data, dataSize, transferIndicator);
///////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////

			//--------------------------assume tombstone members` values
			memcpy((char*)newPage+originalOffset,&transferIndicator.transfer_pageNum,2);
			memcpy((char*)newPage+originalOffset+2,&transferIndicator.transfer_slotNum,2);
			short int tombstoneLength=4;
			//--------------------------------the tombstone`s length is4 bytes----------------
			memcpy((char*)newPage+PAGE_SIZE-4-4*rid.slotNum+2,&tombstoneLength,2);//slot`s recordLength changes into tombstone`s length

			//records移位
			int difference=originalLength-tombstoneLength;
//==========================================================================================================
			for(int i=rid.slotNum+1;i<=slotNum;i++)
			{
				memset(buffer,0,4000);


			   memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4*(i),2);//求出目标record后一条record的offset
			   memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4-4*(i)+2,2);//求出record 后一条record的length

			   if(recordLength==-1){
				   continue;
			   }

			   int newRecordOffset=recordOffset-difference;/////////////////////////2:17 modified

			   memcpy(buffer,(char*)newPage+recordOffset,recordLength);
			   memset((char*)newPage+recordOffset,0,recordLength);//给下一条record移位前空间清零
			   memcpy((char*)newPage+newRecordOffset,buffer,recordLength);//移位
			   memcpy((char*)newPage+PAGE_SIZE-4-4*i,&newRecordOffset,2);//write the new offset into slot



			}

//========================================================================================================

			   freeSpacePointer-=difference;
			   memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);//update the free space pointer;




			fileHandle.writePage(rid.pageNum, newPage);
            free(buffer);
		    free(newPage);
		    return 0;


		}

	}
}

TOMBSTONE RecordBasedFileManager::updateTransferdRecord(FileHandle &fileHandle, const void *data, short int dataSize,TOMBSTONE &transferIndicator, const RID &rid)
{
	void *transferPage=malloc(PAGE_SIZE);
	short int transferOffset,transferLength;
	short int recordLength,recordOffset;
	short int freeSpacePointer,slotNum;
    char* buffer=(char*)malloc(4000);


	fileHandle.readPage(transferIndicator.transfer_pageNum,transferPage);
	memcpy(&freeSpacePointer,(char*)transferPage+PAGE_SIZE-2,2);
	memcpy(&slotNum,(char*)transferPage+PAGE_SIZE-2-2,2);

	memcpy(&transferOffset,(char*)transferPage+PAGE_SIZE-4-4*transferIndicator.transfer_slotNum,2);
	memcpy(&transferLength,(char*)transferPage+PAGE_SIZE-4-4*transferIndicator.transfer_slotNum+2,2);
	if(transferLength>=dataSize)
	{
		//the space enough to update

		memset((char*)transferPage+transferOffset,0,transferLength);//擦除原record
		memcpy((char*)transferPage+transferOffset,data,dataSize);//写上新的record
		memcpy((char*)transferPage+PAGE_SIZE-4-4*transferIndicator.transfer_slotNum+2,&dataSize,2);//更新slot中的record length
		short int difference=transferLength-dataSize;
//=====================================================================================================
		for(int i=transferIndicator.transfer_slotNum+1;i<=slotNum;i++)
		{

		   memset(buffer,0,4000);
		   memcpy(&recordOffset,(char*)transferPage+PAGE_SIZE-4-4*i,2);//求出目标record后一条record的offset
		   memcpy(&recordLength,(char*)transferPage+PAGE_SIZE-4-4*i+2,2);//求出record 后一条record的length
		  //check whether the record has been deleted.
		   if(recordLength==-1){
			   continue;
		   }
		   int newRecordOffset=recordOffset-difference;
		  // memset((char*)newPage+newRecordOffset,0,recordLength);//给下一条record移位前空间清零
		   memcpy(buffer,(char*)transferPage+recordOffset,recordLength);//将record复制到buffer上
		   memset((char*)transferPage+recordOffset,0,recordLength);//清空原位置的record
		   memcpy((char*)transferPage+newRecordOffset,buffer,recordLength);//移位
		   memcpy((char*)transferPage+PAGE_SIZE-4-4*i,&newRecordOffset,2);//write the new offset into slot
		}
//==================================================================================================================

		////////////the transferIndicator`s slotNum and pageNum no change

	   freeSpacePointer-=difference;
	   memcpy((char*)transferPage+PAGE_SIZE-2,&freeSpacePointer,2);//update the free space pointer;

		free(buffer);
		fileHandle.writePage(transferIndicator.transfer_pageNum, transferPage);
		free(transferPage);
		return transferIndicator;


	}

	else//the original transfered position`s space is not enough, we need to find another place
	{
		//delete the transfered record first, and shrink the rest records, then we need to decide whether to insert the neww record in this page.
		TOMBSTONE transferTwiceIndicator;
		if (transferLength > 0) memset((char*)transferPage+transferOffset,0,transferLength);//擦除原record
		short int deletedTombstoneLength=-1;
		memcpy((char*)transferPage+PAGE_SIZE-4-4*transferIndicator.transfer_slotNum+2,&deletedTombstoneLength,2);//slot`s recordLength changes to -1

		//records移位
		int difference=transferLength;
//==========================================================================================================
		for(int i=transferIndicator.transfer_slotNum+1;i<=slotNum;i++)
		{
			memset(buffer,0,4000);


		   memcpy(&recordOffset,(char*)transferPage+PAGE_SIZE-4-4*(i),2);//求出目标record后一条record的offset
		   memcpy(&recordLength,(char*)transferPage+PAGE_SIZE-4-4*(i)+2,2);//求出record 后一条record的length

		   if(recordLength==-1){
			   continue;
		   }

		   int newRecordOffset=recordOffset-difference;/////////////////////////2:17 modified

		   memcpy(buffer,(char*)transferPage+recordOffset,recordLength);
		   memset((char*)transferPage+recordOffset,0,recordLength);//给下一条record移位前空间清零
		   memcpy((char*)transferPage+newRecordOffset,buffer,recordLength);//移位
		   memcpy((char*)transferPage+PAGE_SIZE-4-4*i,&newRecordOffset,2);//write the new offset into slot



		}

//========================================================================================================

		   freeSpacePointer-=difference;
		   memcpy((char*)transferPage+PAGE_SIZE-2,&freeSpacePointer,2);//update the free space pointer;

		   int freeSpace=PAGE_SIZE-freeSpacePointer-4-4*transferIndicator.transfer_slotNum-4;
		   if(freeSpace>=dataSize)
		   {

			   //this page has enough space for the new record, insert the neww record here
			   memcpy((char*)transferPage+freeSpacePointer,data,dataSize);
			   slotNum+=1;
			   //update the new record`s information in its slot
			   memcpy((char*)transferPage+PAGE_SIZE-4-4*slotNum,&freeSpacePointer,2);
			   memcpy((char*)transferPage+PAGE_SIZE-4-4*slotNum+2,&dataSize,2);
               //update the new free space pointer and slotNum in the slot
			   freeSpacePointer+=dataSize;
			   memcpy((char*)transferPage+PAGE_SIZE-2,&freeSpacePointer,2);
			   memcpy((char*)transferPage+PAGE_SIZE-4,&slotNum,2);

			   //update the tombstone`s information:
			   //transferIndicator.transfer_pageNum does not need to change, cause we update the record in the same page.
			   //transferIndicator.transfer_slotNum need to change.
			   transferIndicator.transfer_slotNum=slotNum;
			   fileHandle.writePage(transferIndicator.transfer_pageNum,transferPage);
			   free(transferPage);
			   free(buffer);
			   return transferIndicator;//return struct


		   }

		   else
		   {

			   //this page has no enough space, we need to insert the new record in another page
			   //transfer the new record to another page
		       /////////////////////////////////////////////////////////////////////////////////////////////////////
		       //////////////////////////////////////////////////////////////////////////////////////////////////////
				transferRecord(fileHandle, data, dataSize, transferTwiceIndicator);
		       /////////////////////////////////////////////////////////////////////////////////////////////////////
		       //////////////////////////////////////////////////////////////////////////////////////////////////////
			    fileHandle.writePage(transferIndicator.transfer_pageNum, transferPage);
		        free(buffer);
			    free(transferPage);
			    return transferTwiceIndicator;//return struct !!!!!

		   }

	}

}



RC RecordBasedFileManager::transferRecord(FileHandle &fileHandle, const void *data, short int dataSize, TOMBSTONE &transferIndicator)
{
	{//i=111;

		short int pageNumber=fileHandle.getNumberOfPages()-1;
		//?
		if((pageNumber==fileHandle.getNumberOfPages()-1)&&(pageNumber>=0))
		{
			short int freeSpacePointer, slotNum;
	   		void *newPage=malloc(PAGE_SIZE);
	   		//char* offsetNow=(char*)newPage;
	   		fileHandle.readPage(pageNumber,newPage);
	   		memcpy(&freeSpacePointer,(char*)newPage+PAGE_SIZE-2,2);
	   		memcpy(&slotNum,(char*)newPage+PAGE_SIZE-4,2);
	   	    short int freeSpace=PAGE_SIZE-freeSpacePointer-4-4*slotNum-4;//the -4 means adding a new record need 4 to store its slot
	   	    if(freeSpace>=dataSize)
	   	    {
				slotNum+=1;//slot number +1;
				//offsetNow+=freeSpacePointer;
				memcpy((char*)newPage+freeSpacePointer,data,dataSize);//transfer and then write the record
				transferIndicator.transfer_pageNum=pageNumber;
				transferIndicator.transfer_slotNum=slotNum;

				//increase a slot in the slot catalog------------------------------------------------------
				memcpy((char*)newPage+PAGE_SIZE-4-4*slotNum,&freeSpacePointer,2);//store the transfered record`s offset in the slot
				memcpy((char*)newPage+PAGE_SIZE-4-4*slotNum+2,&dataSize,2);//store the transfered record`s length in the slot



		   		freeSpacePointer=freeSpacePointer+dataSize;
		   		memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);//update free space pointer
		   		memcpy((char*)newPage+PAGE_SIZE-4,&slotNum,2);//update the slotNum in the page
		   		fileHandle.writePage(pageNumber,newPage);
				free(newPage);
				return 0;

	   	    }
	   	    else{
	   	    	fileHandle.writePage(pageNumber,newPage);
	   	    	free(newPage);
	   	    	goto RECURRSIVE;
	   	    }
		}


		RECURRSIVE:
		{

		   for(int i=0;i<fileHandle.getNumberOfPages();i++)
		  {
			        short int freeSpacePointer, slotNum;
			   		void *newPage=malloc(PAGE_SIZE);
			   		//char* offsetNow=(char*)newPage;
			   		fileHandle.readPage(i,newPage);
			   		memcpy(&freeSpacePointer,(char*)newPage+PAGE_SIZE-2,2);
			   		memcpy(&slotNum,(char*)newPage+PAGE_SIZE-4,2);
			   	    int freeSpace=PAGE_SIZE-freeSpacePointer-4-4*slotNum-4;
			   	    if(freeSpace>=dataSize)
			   	    {
			   	    	slotNum+=1;
			   	    	//offsetNow+=freeSpacePointer;
			   	    	//-----------------------------------------
						memcpy((char*)newPage+freeSpacePointer,data,dataSize);//transfer and then write the record
						transferIndicator.transfer_pageNum=i;
						transferIndicator.transfer_slotNum=slotNum;

						//-----------------------------------------------

						//add a slot in the slot catalog
						memcpy((char*)newPage+PAGE_SIZE-4-4*slotNum,&freeSpacePointer,2);//store the transfered record`s offset in the slot
						memcpy((char*)newPage+PAGE_SIZE-4-4*slotNum+2,&dataSize,2);//store the transfered record`s length in the slot


			  	   		freeSpacePointer=freeSpacePointer+dataSize;
				   		memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);//update free space pointer
				   		memcpy((char*)newPage+PAGE_SIZE-4,&slotNum,2);//update the slotNum in the page
			   	   		fileHandle.writePage(i,newPage);
			   	   		free(newPage);
			   	 	    return 0;
			   	   		//break;


			   	    }
			   	    else
			   	    {
			   	    	free(newPage);

			   	    	//===========================================
			   	    	if(i=(fileHandle.getNumberOfPages())-1){
			   	    		break;
			   	    	}

			   	    	else{
			   	    		continue;
			   	    	}
			   	    	//============================================

			   	    }
		  }

//		                cout<<"5555555555555555555555555555"<<endl;
			 		    short int freeSpacePointer=0, slotNum=0;
			 		    short int recordOffset, recordLength;
			 		   	void *newPage=malloc(PAGE_SIZE);
			 		   	memset(newPage,0,PAGE_SIZE);
			 		   	fileHandle.appendPage(newPage);
			   	    	int newPageNum=fileHandle.getNumberOfPages()-1;
			 		   	fileHandle.readPage(newPageNum,newPage);
				   		//memcpy(&freeSpacePointer,(char*)newPage+PAGE_SIZE-2,2);
				   		//memcpy(&slotNum,(char*)newPage+PAGE_SIZE-4,2);
	                    slotNum+=1;

	                    recordOffset=freeSpacePointer;
	                    recordLength=dataSize;
			   	    	//-----------------------------------------
						memcpy((char*)newPage+recordOffset,data,dataSize);//transfer and then write the record
						transferIndicator.transfer_pageNum=newPageNum;
						transferIndicator.transfer_slotNum=slotNum;

						//-----------------------------------------------
						//add a slot in the slot catalog
						memcpy((char*)newPage+PAGE_SIZE-4-4*slotNum,&recordOffset,2);//store the transfered record`s offset in the slot
						memcpy((char*)newPage+PAGE_SIZE-4-4*slotNum+2,&recordLength,2);//store the transfered record`s length in the slot
                        //--------------------------------------------------------
 			 		   	freeSpacePointer+=recordLength;
 			 	        memcpy((char*)newPage+PAGE_SIZE-2,&freeSpacePointer,2);//update free space pointer
				   		memcpy((char*)newPage+PAGE_SIZE-4,&slotNum,2);//update the slotNum in the page

			 	        fileHandle.writePage(newPageNum,newPage);
			 	    	free(newPage);
			 	   	    return 0;

	}
	}


}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const RID &rid,const string &attributeName, void *data)
{
	void *buffer = malloc(PAGE_SIZE);
    readRecord(fileHandle,recordDescriptor,rid,buffer);
    printRecord(recordDescriptor,buffer);
	int offset=ceil(recordDescriptor.size()/8.0);


	//-------------------------nullIndicator-------------------------------------

	int nullAttributesIndicatorActualSize=ceil(recordDescriptor.size()/8.0);
	memcpy(data,buffer,nullAttributesIndicatorActualSize);
	char* nullDirectory = (char*) malloc(nullAttributesIndicatorActualSize);
	memcpy(nullDirectory, (char*) buffer, nullAttributesIndicatorActualSize);
	int nullBitNum;
	int nullIndicatorByte;

	//------------------------------------------------------------------------------

	char conditioncopy[50] = "                                                 ";
	char *str2;

	for (int i = 0; i < recordDescriptor.size(); i++) {

//---------------------------check that whether this field is null-----------------
		nullIndicatorByte = i / 8;
		nullBitNum = i % 8;
		if (nullDirectory[nullIndicatorByte] & (1 << 7 - nullBitNum))
		{
			continue;
		}

//--------------------------------------------------------------------------------------------

	//	const int p_lengthc = attributeName.length();
	//	str2 = new char[p_lengthc+1];
	//	strcpy(str2,attributeName.c_str());
	//	memcpy(conditioncopy,str2,p_lengthc);


		 if (attributeName==recordDescriptor[i].name)
		 {
				switch (recordDescriptor[i].type)
				{

				case TypeInt:
				{
					memcpy((char*)data+nullAttributesIndicatorActualSize,(char*)buffer+offset,4);
					break;
				}
				case TypeReal:
				{
					memcpy((char*)data+nullAttributesIndicatorActualSize,(char*)buffer+offset,4);
					break;
				}
				case TypeVarChar:
				{
					int varcharLength;
					memcpy(&varcharLength,(char*)buffer+offset,4);
					memcpy((char*)data+nullAttributesIndicatorActualSize,(char*)buffer+offset,4+varcharLength);
					break;

				}

			}

		}

		else
		{
						switch (recordDescriptor[i].type) {

						case TypeInt: {
							offset += 4;
	//						 cout<<"offset is "<<offset<<endl;

							break;
						}
						case TypeReal: {
							offset += 4;
	//						 cout<<"offset is "<<offset<<endl;

							break;
						}
						case TypeVarChar: {
							int var;;
							memcpy(&var, (char*) buffer + offset, 4);
							offset += 4;
							offset += var;
	//						 cout<<"offset is "<<offset<<endl;

							break;

						}


						}

		}

	}
	free(buffer);
	return 0;

}




RC RecordBasedFileManager:: scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator)
{
	// offer value to rbfm_ScanIterator
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.conditionAttrbutestr = conditionAttribute;
	rbfm_ScanIterator.attributeNames_iterator = attributeNames;
	rbfm_ScanIterator.value = value;
	rbfm_ScanIterator.recordDescriptor_iterator = recordDescriptor;
	RID rid_scan;
	rid_scan.pageNum =0;
	rid_scan.slotNum =0;//in iterator it add 1; so that every recall will begin in a new slot
	rbfm_ScanIterator.fileHandlescaniter = fileHandle;
	rbfm_ScanIterator.pagenum = 0;
	rbfm_ScanIterator.rid_iterator =rid_scan;




	return 0;
}

RC RBFM_ScanIterator::scan_compare(const CompOp compOp,int &hit,const void*value,const void*value_get)
{
	switch(compOp)
	{
		case LT_OP:
		{
			if(value_get<value)// is value is varchar????????????????
			{
				hit=1;
			}
			else
			{
				hit =0;
			}
			break;
		}
		case LE_OP:
		{
			if(value_get<=value)
			{
				hit=1;
			}
			else
			{
				hit =0;
			}
			break;
		}
		case GT_OP:
		{
			if(value_get>value)
			{
				hit=1;
			}
			else
			{
				hit =0;
			}
			break;
		}
		case GE_OP:
		{
			if(value_get>=value)
			{
				hit=1;
			}
			else
			{
				hit =0;
			}
			break;
		}
		case NE_OP:
		{
			if(value_get!=value)
			{
				hit=1;
			}
			else
			{
				hit =0;
			}
			break;
		}
		case NO_OP:
		{
			hit=1;
			break;
		}
		default :
			return -1;
	}
	return 0;
}

RC RBFM_ScanIterator::scan_readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid,const void *data,void* read_data)
{
	void* buffer=malloc(1000);
	char* pointer=(char* )data;
    short int recordLength,recordOffset ;
    pointer=pointer+PAGE_SIZE-4-4*rid.slotNum;
    memcpy(&recordOffset,pointer,2);
    pointer+=2;
    memcpy(&recordLength,pointer,2);
    int nullIndicator=ceil((double)recordDescriptor.size()/8.0);
    memcpy((char*)buffer,(char*)data+recordOffset, recordLength);
	memcpy(read_data,buffer,nullIndicator);//write the NULL indicator into data.
    int offsetNow=ceil((double)recordDescriptor.size()/8.0);

    int offset=ceil((double)recordDescriptor.size()/8.0);

    for(unsigned int i=0;i<recordDescriptor.size();i++)
    {



		switch(recordDescriptor[i].type){
		case TypeInt:{
			int intField;
			memcpy(&intField,offset+(char*)buffer,4);
            //cout<<recordDescriptor[i].name<<":"<<intField<<endl;
			offset+=4;
			memcpy((char*)read_data+offsetNow,&intField,4);
			offsetNow+=4;
			//fieldLength += recordDescriptor[i].length;
			break;
		}
		case TypeReal:{
			float realField;
			memcpy(&realField,offset+(char*)buffer,4);
			offset+=4;
			memcpy((char*)read_data+offsetNow,&realField,4);//write the real type field into data.
			offsetNow+=4;
			//cout<<recordDescriptor[i].name<<":"<<realField<<endl;
			break;
		}
		case TypeVarChar:{
			int varcharFieldLength;
			char* varcharField;
			memcpy(&varcharFieldLength,(char*)buffer+offset,4);
			memcpy((char*)read_data+offsetNow,(char*)buffer+offset,4);//write the length information of varchar filed into data.
			offset+=4;
			varcharField=(char*)malloc(varcharFieldLength);
			memcpy(varcharField,offset+(char*)buffer,varcharFieldLength);
			//cout<<recordDescriptor[i].name<<":"<<varcharField<<endl;
			offset+=varcharFieldLength;
			offsetNow+=4;
			memcpy((char*)read_data+offsetNow,varcharField,varcharFieldLength);//write the text information of varchar field into data.
			offsetNow+=varcharFieldLength;
			free(varcharField);
			break;
		}
		}



    }

    free(buffer);
    return 0;
}

///////////////////////iterator
RC RBFM_ScanIterator:: getNextRecord(RID &rid, void *data)
{
	char* data_store =(char*) malloc(PAGE_SIZE); //need free in the end
	//char* pointer = (char*) newPage;
	memset(data_store,0,PAGE_SIZE);
	int size_record = 0;
	int hit =0;// show this record fit requirement
	int nullBit=ceil((double)recordDescriptor_iterator.size()/8.0);
//	vector<Attribute> seleterecordDescriptor;// store new attributename->attr
//	cout<<pagenum<<"pagenum"<<endl;
//	cout<<fileHandlescaniter.getNumberOfPages()<<" get pagenum"<<endl;//page ++strange!
//	cout<<fileHandlescaniter.getNumberOfPages()<<"fileHandlescaniter.getNumberOfPages()"<<endl;
//	cout<<pagenum<<"pagenum"<<endl;
	for(pagenum;pagenum<fileHandlescaniter.getNumberOfPages();)
	{
		rid_iterator.pageNum = pagenum;
	//	cout<<"pagenum"<<pagenum<<endl;
		fileHandlescaniter.readPage(pagenum,data_store);
		short int slotall;
		int offset=PAGE_SIZE-4;

		memcpy(&slotall,offset+(char*)data_store,2);//get all slot num
	//	cout<<slotall<<"slotall"<<endl;
	//	cout<<rid_iterator.slotNum<<"rid_iterator.slotNum"<<endl;
		if(rid_iterator.slotNum+1>slotall)
		{
	//		cout<<"add page"<<endl;
			pagenum++;
	//		cout<<"add page num"<<pagenum<<endl;
			rid_iterator.slotNum = 0;
			continue; // go to next page
		}
		else
		{
			//cout<<"searcg this page"<<endl;
			for(rid_iterator.slotNum = rid_iterator.slotNum+1;rid_iterator.slotNum <=slotall;rid_iterator.slotNum++)
			{
				short int recordlength = 0;
				int position = 0;
				offset = offset - rid_iterator.slotNum*4+2;
				memcpy(&recordlength,offset+(char*)data_store,2);// get the length
		//		cout<<recordlength<<"recordlengthhhhhhhhhhhhhhhhhhhh"<<endl;
				if(recordlength<=4)//==-1||recordlength==0
				{
//					cout<<"break this"<<endl;
		//			cout<<"t stone"<<endl;
					break;
				}
				char*read_data =(char*) malloc(recordlength);
		//		cout<<"here"<<endl;
		//		 cout<<recordDescriptor_iterator[1].name<<"recordDescriptor_iterator[j].name"<<endl;
				scan_readRecord(fileHandlescaniter,recordDescriptor_iterator,rid_iterator,data_store,read_data);//should rewrite readRecord function? Since this does not use pagenum, and will not only read one record
	//			cout<<"there"<<endl;
				//				char*read_data2 =(char*) malloc(recordlength);
//				scan_readRecord(fileHandlescaniter,recordDescriptor_iterator,rid_iterator,data_store,read_data2);
//				free(read_data2);
		//		cout<<recordDescriptor_iterator.size()<<"recordDescriptor_iterator.size()"<<endl;
				/*
				int varcharFieldLength3;
				char* varcharField3;
				memcpy(&varcharFieldLength3,(char*)read_data+4,4);
				int aa =0;
				aa+=varcharFieldLength3;
				varcharField3=(char*)malloc(varcharFieldLength3);
				memset(varcharField3,0,varcharFieldLength3);
				cout<<"sizeee"<<varcharFieldLength3<<endl;
				memcpy(varcharField3,aa+4+(char*)read_data,varcharFieldLength3);
				cout<<"sizeeeCHARRRRRRRRRRRRRRRRRRRRRRRRRRR"<<varcharField3<<endl;
				free(varcharField3);
*/

				//begin to compare

				if(conditionAttrbutestr == "")
				{
					rid.pageNum = rid_iterator.pageNum;
					rid.slotNum = rid_iterator.slotNum;
					//data = read_data;//?true???
					hit =1;
					//return 0;
					//no condition
				}
				else
				{
					char conditioncopy[50] = "                                                 ";
					char *str2;
					const int p_lengthc = conditionAttrbutestr.length();
					str2 = new char[p_lengthc+1];

					strcpy(str2,conditionAttrbutestr.c_str());
					memcpy(conditioncopy,str2,sizeof(conditionAttrbutestr));//sizeof or length??seems can get different byte......


					int offsetgetvalue=0;// attrs's position offset

					offsetgetvalue=offsetgetvalue+nullBit;
					for(unsigned int j=0;j<recordDescriptor_iterator.size();j++)
					{
						if(recordDescriptor_iterator[j].name == conditionAttrbutestr)
						{
							position = j+1;// true position
							break;
						}
						if(position == 0)
						{
							return -1;//fail to find
						}
						// already find position
						//not consider NULL situation firstly
						//begin to get the position of value

						for(unsigned int i=0;i<position-1;i++)
						{
							switch(recordDescriptor_iterator[i].type)
							{
								case TypeInt:{
									offsetgetvalue+= 4;
									break;
								}
								case TypeReal:{
									offsetgetvalue+=4;
									break;
								}
								case TypeVarChar:{
									int varcharFieldLength;
									char* varcharField;
									memcpy(&varcharFieldLength,(char*)read_data+offsetgetvalue,4);
									offsetgetvalue+=4;
									varcharField=(char*)malloc(varcharFieldLength);
									memcpy(varcharField,offsetgetvalue+(char*)read_data,varcharFieldLength);
									offsetgetvalue+=varcharFieldLength;
									free(varcharField);
									break;
								}
								default :
									return -1;
							}
						}
					// to the position
					//	void* value_get; compare if meet comp
						switch(recordDescriptor_iterator[position-1].type){
							case TypeInt:{
								int intField;
								memcpy(&intField,offsetgetvalue+(char*)read_data,4);
								scan_compare(compOp,hit,value,&intField);
								break;
							}
							case TypeReal:{
								float realField;
								memcpy(&realField,offsetgetvalue+(char*)read_data,4);
								scan_compare(compOp,hit,value,&realField);
								break;
							}
							case TypeVarChar:{
								int varcharFieldLength2;
								char* varcharField2;
								memcpy(&varcharFieldLength2,(char*)read_data+offsetgetvalue,4);
								offset+=4;
								varcharField2=(char*)malloc(varcharFieldLength2);
								memcpy(varcharField2,offset+(char*)read_data,varcharFieldLength2);
								scan_compare(compOp,hit,value,varcharField2);//compare value && varchar  maybe problem, varchar contain "   "

								free(varcharField2);
								break;
							}
							default :
								return -1;

						}
					}
				}//end else....



//cout<<"hit"<<endl;

					//get the value in position
					// value may cause problem
					//judge whether hit =1; the reocrd satisified
					//malloc PAGE_SIZE full page -> write
					if(hit==0)
					{
						break;// turn to next data
					}
					else
					{
						int size_attname =attributeNames_iterator.size();
						int getatt =0;
						int offset_abs =nullBit;//did not consider null
						int counter =0;
						int recordleng1 = 0;//length
		//				cout<<size_attname<<"size_attname"<<endl;
						while(getatt<size_attname)//find abstract data's length to malloc
						{
							switch(recordDescriptor_iterator[counter].type){

								case TypeInt:{
									int intField;
									memcpy(&intField,offset_abs+(char*)read_data,4);
									for(int inum=0;inum<size_attname;inum++)
									{
										char attcopy1[50] = "                                                 ";
										char *str3;
										const int p_lengthc = attributeNames_iterator[inum].length();
										str3 = new char[p_lengthc+1];
										strcpy(str3,attributeNames_iterator[inum].c_str());
										memcpy(attcopy1,str3,strlen(str3));//sizeof or length??seems can get different byte......
									//	cout<<"str3"<<str3<<endl<<"name  "<<recordDescriptor_iterator[counter].name<<endl<<"attcopy           "<<attcopy1<<endl;
										if(recordDescriptor_iterator[counter].name==str3)
										{
											getatt++;
										//	cout<<"JUDGE int"<<endl;
											recordleng1 = recordleng1+4;
										}
									}

									offset_abs = offset_abs +4;
								//    cout<<recordDescriptor[i].name<<":"<<intField<<endl;
			//						*value_get = intField;
									//fieldLength += recordDescriptor[i].length;
									break;
								}
								case TypeReal:{
									float realField;
									memcpy(&realField,offset_abs+(char*)read_data,4);
									for(int inum=0;inum<size_attname;inum++)
									{
										char attcopy2[50] = "                                                 ";
										char *str3;
										const int p_lengthc = attributeNames_iterator[inum].length();
										str3 = new char[p_lengthc+1];
										strcpy(str3,attributeNames_iterator[inum].c_str());
										memcpy(attcopy2,str3,strlen(str3));//sizeof or length??seems can get different byte......
									//	cout<<"name  "<<recordDescriptor_iterator[counter].name<<endl<<"attcopy           "<<attcopy2<<endl;
										if(recordDescriptor_iterator[counter].name==str3)
										{
											getatt++;
									//		cout<<"JUDGE real"<<endl;
											recordleng1 = recordleng1+4;
										}
									}

									offset_abs = offset_abs +4;
								//	cout<<recordDescriptor[i].name<<":"<<realField<<endl;
									break;
								}
								case TypeVarChar:{
								//	cout<<"enter varchar"<<endl;
									int varcharFieldLength2;
									char* varcharField4;
									memcpy(&varcharFieldLength2,(char*)read_data+offset_abs,4);
									offset_abs = offset_abs+4+varcharFieldLength2;
									varcharField4=(char*)malloc(varcharFieldLength2);
									for(int inum=0;inum<size_attname;inum++)
									{
										char attcopy3[50] = "                                                 ";
										char *str3;
										const int p_lengthc = attributeNames_iterator[inum].length();
										str3 = new char[p_lengthc+1];
										strcpy(str3,attributeNames_iterator[inum].c_str());
										memcpy(attcopy3,str3,strlen(str3));//sizeof or length??seems can get different byte......
								//		cout<<"name  "<<recordDescriptor_iterator[counter].name<<endl<<"attcopy           "<<attcopy3<<endl;
										if(recordDescriptor_iterator[counter].name==str3)
										{
											getatt++;
								//			cout<<"JUDGE var"<<endl;
											recordleng1 = 4+recordleng1+varcharFieldLength2;
										}
									}
			//						*value_get = varcharField2
							//		cout<<recordDescriptor[i].name<<":"<<varcharField2<<endl;

									free(varcharField4);
									break;
								}
								default :
									return -1;

							}
							counter++;
							if(counter>=recordDescriptor_iterator.size())
							{
								break;
							}
						}

						//already get length...
						//combine to a record
						//already get length...
						//combine to a record
						//                  fail to get record!
						int renullIndicatorbyte=ceil((double)attributeNames_iterator.size()/8.0);
						unsigned char *renullsIndicator = (unsigned char *) malloc(renullIndicatorbyte);
						memset(renullsIndicator,0,renullIndicatorbyte);
						recordleng1 = recordleng1+renullIndicatorbyte;
						char * abstractrecord = (char*)malloc(recordleng1);
						memcpy(abstractrecord, renullsIndicator,renullIndicatorbyte);
	//					cout<<recordleng1<<"length"<<endl;
						int offsetaddrecoed = renullIndicatorbyte;
						unsigned int counter_com =0;
	//					cout<<nullBit<<"nullBitnullBitnullBitnullBitnullBitnullBit"<<endl;
						offset_abs = nullBit;
						getatt = 0;

//						Attribute seleterecordatt;
							while(getatt<size_attname)//find abstract data's length to malloc
							{
	//							cout<<"enter offset"<<offset_abs<<endl;
	//							cout<<"recordDescriptor_iterator[counter_com].name"<<recordDescriptor_iterator[counter_com].name<<endl;

								switch(recordDescriptor_iterator[counter_com].type){
									case TypeInt:{
										int intField;
										for(int inum=0;inum<size_attname;inum++)
										{
//											cout<<"attributeNames_iterator[inum]"<<attributeNames_iterator[inum]<<endl;
											char attcopy[50] = "                                                 ";
											char *str3;
											const int p_lengthc = attributeNames_iterator[inum].length();
											str3 = new char[p_lengthc+1];
											strcpy(str3,attributeNames_iterator[inum].c_str());
											memcpy(attcopy,str3,strlen(str3));//sizeof or length??seems can get different byte......
//											cout<<"name  "<<recordDescriptor_iterator[counter_com].name<<endl<<"attcopy           "<<attcopy<<endl;
											memcpy(&intField,offset_abs+(char*)read_data,4);
	//										cout<<"int age "<<intField<<endl;
	//										cout<<"int aaaaaaaaaaaaaaaaaaaaaaaaaaaa "<<endl;
											if(recordDescriptor_iterator[counter_com].name==str3)
											{
//												cout<<"enter INTTTTTTTTTTTTTTTTTTTTTT"<<endl;
												getatt++;
												memcpy(&intField,offset_abs+(char*)read_data,4);
			//									cout<<"int age "<<intField<<endl;
												memcpy(abstractrecord+offsetaddrecoed, &intField,4);
												offsetaddrecoed = offsetaddrecoed +4;
//												seleterecordatt.name = attcopy;
//												seleterecordatt.type = TypeInt;
//												seleterecordatt.length = 4;
//												seleterecordDescriptor.push_back(seleterecordatt);
											}
										}

										offset_abs = offset_abs +4;
									//    cout<<recordDescriptor[i].name<<":"<<intField<<endl;
				//						*value_get = intField;
										//fieldLength += recordDescriptor[i].length;
										break;
									}
									case TypeReal:{
										float realField;

										for(int inum=0;inum<size_attname;inum++)
										{
//											cout<<"attributeNames_iterator[inum]"<<attributeNames_iterator[inum]<<endl;
											char attcopy[50] = "                                                 ";
											char *str3;
											const int p_lengthc = attributeNames_iterator[inum].length();
											str3 = new char[p_lengthc+1];
											strcpy(str3,attributeNames_iterator[inum].c_str());
											memcpy(attcopy,str3,strlen(str3));//sizeof or length??seems can get different byte......
											if(recordDescriptor_iterator[counter_com].name==str3)
											{
//												cout<<"enter REALLLLLLLLLLLLLLLLLLL"<<endl;
												getatt++;
												memcpy(&realField,offset_abs+(char*)read_data,4);
												memcpy(abstractrecord+offsetaddrecoed, &realField,4);
												offsetaddrecoed = offsetaddrecoed +4;

											}
										}

										offset_abs = offset_abs +4;
									//	cout<<recordDescriptor[i].name<<":"<<realField<<endl;
										break;
									}
									case TypeVarChar:{
//										cout<<"enter varchar"<<endl;
										int varcharFieldLength2;
										char* varcharField2;
										memcpy(&varcharFieldLength2,(char*)read_data+offset_abs,4);
										offset_abs+=4;
										varcharField2=(char*)malloc(varcharFieldLength2);
										memset(varcharField2,0,varcharFieldLength2);
				//						cout<<"sizeee"<<varcharFieldLength2<<endl;
										for(int inum=0;inum<size_attname;inum++)
										{
//											cout<<"attributeNames_iterator[inum]"<<attributeNames_iterator[inum]<<endl;
											char attcopy[50] = "                                                 ";
											char *str3;
											const int p_lengthc = attributeNames_iterator[inum].length();
											str3 = new char[p_lengthc+1];
											strcpy(str3,attributeNames_iterator[inum].c_str());
											memcpy(attcopy,str3,strlen(str3));//sizeof or length??seems can get different byte......
											if(recordDescriptor_iterator[counter_com].name==str3)
											{
//												cout<<"enter VARCHARRRRRRRRRRRRR"<<endl;
												getatt++;
												memcpy(abstractrecord+offsetaddrecoed, &varcharFieldLength2,4);
		//										cout<<"VARCHAR LENGTH              LENGTH"<<varcharFieldLength2<<endl;
												offsetaddrecoed = offsetaddrecoed +4;
												memcpy(varcharField2,offset_abs+(char*)read_data,varcharFieldLength2);
			//									cout<<"DATA              LENGTH"<<varcharField2<<endl;
												memcpy((char*)abstractrecord+offsetaddrecoed, varcharField2,varcharFieldLength2);
												//



												char* varcharField33;
												varcharField33=(char*)malloc(varcharFieldLength2);
												memset(varcharField33,0,varcharFieldLength2);
			//									cout<<"offsetaddrecoed"<<offsetaddrecoed<<endl;
			//									cout<<"varcharFieldLength2"<<varcharFieldLength2<<endl;
												memcpy(varcharField33,offsetaddrecoed+(char*)abstractrecord,varcharFieldLength2);
			//									cout<<"sizeeeCHARRRRRRRRRRRRRRRRRRRRRRRRRRR    "<<varcharField33<<endl;
												free(varcharField33);

												offsetaddrecoed = offsetaddrecoed +varcharFieldLength2;


	//											cout<<"offsetaddrecoed"<<offsetaddrecoed<<endl;
											}
										}

				//						*value_get = varcharField2
										memcpy(varcharField2,offset_abs+(char*)read_data,varcharFieldLength2);
//										cout<<"varchar ffffffffffffffffffffffffffffffffffffff"<<varcharFieldLength2<<":"<<varcharField2<<endl;
										offset_abs+=varcharFieldLength2;
										free(varcharField2);
										break;
									}
									default :
										return -1;

								}

								counter_com++;
//								cout<<getatt<<"getattvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"<<endl;
								if(counter_com>=recordDescriptor_iterator.size())
								{
									break;
								}

							}
//							cout<<"END COMBINEEEEEE"<<endl;
							rid.pageNum = rid_iterator.pageNum;
							rid.slotNum = rid_iterator.slotNum;
						//	cout<<"SIZEEEEEEEEEEEEE"<<(sizeof(read_data)/sizeof(*read_data))<<endl;
	//						cout<<"END recordlength"<<recordleng1<<endl;
					//		cout<<"DATA AGE REA"<<(char *)(abstractrecord+4)<<endl;

/*
							int varcharFieldLength3 =0 ;
//							char* varcharField3;
							memcpy(&varcharFieldLength3,(char*)abstractrecord+1+4,4);
		//					cout<<"testttttttttttttttt"<<varcharFieldLength3<<endl;
							//							int aa =0;
//							aa+=varcharFieldLength3;
							char* varcharField33;
							varcharField33=(char*)malloc(varcharFieldLength3);
							memset(varcharField33,0,varcharFieldLength3);
//							cout<<"sizeee"<<varcharFieldLength3<<endl;
							memcpy(varcharField33,9+(char*)abstractrecord,varcharFieldLength3);
		//					cout<<"sizeeeCHARRRRRRRRRRRVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV   "<<varcharField33<<endl;
							free(varcharField33);
*/


					//		cout<<"DATA AGE REA DATA  "<<*(int *)((char *)data+1)<<endl;
							//data = abstractrecord;  //why this way fail???!!!!!!!!!!!!!!!!!!!!!!!!!!!!
							memcpy(data,abstractrecord,recordleng1);

			//				cout<<"copy"<<endl;
//							cout<<"DATA AGE REA"<<*(int *)((char *)abstractrecord+1)<<endl;
//							cout<<"DATA AGE REA DATA  "<<*(int *)((char *)data+1)<<endl;
//							cout<<"get size of the data"<<sizeof(abstractrecord)<<endl;
							memset(abstractrecord,0,recordleng1);
							free(abstractrecord);
							free(data_store);
							free(read_data);

							//	return 0;
					}

//				cout<<rid_iterator.slotNum<<"slot num in every cycle"<<endl;
//				cout<<"hit"<<hit<<endl;
				if(hit==1)
				{
					break;
				}
			}
//			cout<<"end of search"<<endl;
			if(hit==1)
			{
				break;
			}
		}
	}
	if(pagenum>=fileHandlescaniter.getNumberOfPages())
	{
		hit = 0;
		return RBFM_EOF;
	}
//cout<<"finish"<<endl;
	//	cout<<"DATA AGE REA DATA IN THE   END  "<<*(int *)((char *)data+1)<<endl;
//	cout<<"END"<<endl;
	if(hit==1)
	{
		return 0;
	}
	else
	{
		return RBFM_EOF;
	}

}


RBFM_ScanIterator::RBFM_ScanIterator()
{
	rid_iterator.pageNum =0;
	rid_iterator.slotNum =0;

}
RBFM_ScanIterator::~RBFM_ScanIterator()
{
	rid_iterator.pageNum =0;
	rid_iterator.slotNum =0;
}




RC RBFM_ScanIterator:: close()
{
	PagedFileManager::instance()->closeFile(fileHandlescaniter);
}


