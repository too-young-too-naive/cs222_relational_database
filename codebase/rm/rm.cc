
#include "rm.h"
#include <vector>
#include <string>


RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	//create a file to store table
	//vector read write in tablel
	FILE* tablefile;
	RID ridcreate;
	FileHandle filehandlecreate;
	RecordBasedFileManager:: instance()->createFile("Tables");
	RecordBasedFileManager:: instance()->openFile("Tables",filehandlecreate);

	Tables lists;
	lists.tableid = 1;
	vector<Attribute> recordDescriptor;
		char *str = "Tables";
		memcpy(lists.tablename,str,strlen(str));
		char *str2 = "Tables";
		memcpy(lists.filename,str2,strlen(str2));
		table.push_back(lists);
//		lists.tablename = name1;

	    Attribute attr;
	    attr.name = "table-id";
	    attr.type = TypeInt;
	    attr.length = (AttrLength)4;
	    recordDescriptor.push_back(attr);
	    Attribute attr2;
	    attr2.name = "table-name";
	    attr2.type = TypeVarChar;
	    attr2.length = (AttrLength)50;
	    recordDescriptor.push_back(attr2);
	    Attribute attr3;
	    attr3.name = "file-name";
	    attr3.type = TypeVarChar;
	    attr3.length = (AttrLength)50;
	    recordDescriptor.push_back(attr3);


		Tables lists2;
		lists2.tableid = 2;
		char *str3 = "Columns";
		memcpy(lists2.tablename,str3,strlen(str3));
		char *str4 = "Columns";
		memcpy(lists2.filename,str4,strlen(str4));
		table.push_back(lists2);

		int size = table.size();
//		cout<<"size"<<table.size()<<endl;
		Tables it;
		int i =0;

		for(i=0;i<size;i++)
		 {
			void* tablerecord = (void*)malloc(113);
			int nulla =0;
			memcpy(tablerecord,&nulla,1);
			it = table[i];
			void *ID = malloc(4);
			memset(ID, 0, sizeof(int));
			*(int*)ID = it.tableid;
	//		cout<<"ID when create"<<*(int*)ID<<endl;

			memcpy((char*)tablerecord+1,(char*)ID,4);
			free(ID);

			int lengtha =50;//value is wrong
			memcpy((char*)tablerecord+5,&lengtha,4);//size of int
			char* nametable = (char*)malloc(50);
			memset(nametable, 0, 50);
			memcpy(nametable,it.tablename,50);
			memcpy((char*)tablerecord+9,(char*)nametable,50);
			free(nametable);

			int lengthb =50;
			memcpy((char*)tablerecord+59,&lengthb,4);
			char* namefile = (char*)malloc(50);
			memset(namefile, 0, 50);
			memcpy(namefile,it.filename,50);
			memcpy((char*)tablerecord+63,namefile,50);
			free(namefile);

			char* nametablea = (char*)malloc(50);
			memcpy((char*)nametablea,(char*)tablerecord+9,50);
	//		cout<<nametablea<<"nametablea  "<<endl;
			free(nametablea);

			RecordBasedFileManager:: instance()->insertRecord(filehandlecreate, recordDescriptor, tablerecord, ridcreate);
/*
			void*datatest = malloc(113);
			memset(datatest,0,113);
			RecordBasedFileManager::instance()->readRecord(filehandlecreate, recordDescriptor, ridcreate, datatest);
			char*readtable = (char*)malloc(50);
			memset(readtable,0,50);
			memcpy(readtable,(char*)datatest+9,50);
			cout<<readtable<<"readtableaxzxzx"<<endl;
			free(readtable);
			free(datatest);
*/
		//	cout<<"slotnum rid"<<ridcreate.slotNum<<endl;
		//	cout<<"rid pagenum"<<ridcreate.pageNum<<endl;
			free(tablerecord);
		 }
		void*datatest = malloc(113);
		memset(datatest,0,113);
		ridcreate.slotNum =1;
		ridcreate.pageNum=0;
		RecordBasedFileManager::instance()->readRecord(filehandlecreate, recordDescriptor, ridcreate, datatest);
		char*readtable = (char*)malloc(50);
		int ass =0;
		memset(readtable,0,50);
		memcpy(&ass,(char*)datatest+5,4);
//		cout<<ass<<"readtableaxzxzx"<<endl;
		free(readtable);
		free(datatest);
		RecordBasedFileManager:: instance()->closeFile(filehandlecreate);


		//////////column
		vector<Attribute> recordDescriptorcolumn;
	    Attribute attrc;
	    attrc.name = "table-id";
	    attrc.type = TypeInt;
	    attrc.length = (AttrLength)4;
	    recordDescriptorcolumn.push_back(attrc);

	    Attribute attrc2;
	    attrc2.name = "column-name";
	    attrc2.type = TypeVarChar;
	    attrc2.length = (AttrLength)50;
	    recordDescriptorcolumn.push_back(attrc2);

	    Attribute attrc3;
	    attrc3.name = "column-type";
	    attrc3.type = TypeInt;
	    attrc3.length = (AttrLength)4;
	    recordDescriptorcolumn.push_back(attrc3);

	    Attribute attrc4;
	    attrc4.name = "column-length";
	    attrc4.type = TypeInt;
	    attrc4.length = (AttrLength)4;
	    recordDescriptorcolumn.push_back(attrc4);

	    Attribute attrc5;
	    attrc5.name = "column-position";
	    attrc5.type = TypeInt;
	    attrc5.length = (AttrLength)4;
	    recordDescriptorcolumn.push_back(attrc5);

		Columns colu1;

		colu1.tableid = 1;
		char *str5 = "table-id";
		memcpy(colu1.columnname,str5,strlen(str5));
		colu1.columntype = TypeInt;
		colu1.columnlength = 4;
		colu1.columnposition = 1;
		column.push_back(colu1);

		Columns colu2;

		colu2.tableid = 1;
		char *str6 = "table-name";
		memcpy(colu2.columnname,str6,strlen(str6));
		colu2.columntype = TypeVarChar;
		colu2.columnlength = 50;
		colu2.columnposition = 2;
		column.push_back(colu2);

		Columns colu3;

		colu3.tableid = 1;
		char *str7 = "file-name";
		memcpy(colu3.columnname,str7,strlen(str7));
		colu3.columntype = TypeVarChar;
		colu3.columnlength = 50;
		colu3.columnposition = 3;
		column.push_back(colu3);

		Columns colu4;

		colu4.tableid = 2;
		char *str8 = "table-id";
		memcpy(colu4.columnname,str8,strlen(str8));
		colu4.columntype = TypeInt;
		colu4.columnlength = 4;
		colu4.columnposition = 1;
		column.push_back(colu4);

		Columns colu5;

		colu5.tableid = 2;
		char *str9 = "column-name";
		memcpy(colu5.columnname,str9,strlen(str9));
		colu5.columntype = TypeVarChar;
		colu5.columnlength = 50;
		colu5.columnposition = 2;
		column.push_back(colu5);

		Columns colu6;

		colu6.tableid = 2;
		char *str10 = "column-type";
		memcpy(colu6.columnname,str10,strlen(str10));
		colu6.columntype = TypeInt;
		colu6.columnlength = 4;
		colu6.columnposition = 3;
		column.push_back(colu6);

		Columns colu7;

		colu7.tableid = 2;
		char *str11 = "column-length";
		memcpy(colu7.columnname,str11,strlen(str11));
		colu7.columntype = TypeInt;
		colu7.columnlength = 4;
		colu7.columnposition = 4;
		column.push_back(colu7);

		Columns colu8;

		colu8.tableid = 2;
		char *str12 = "column-position";
		memcpy(colu8.columnname,str12,strlen(str12));
		colu8.columntype = TypeInt;
		colu8.columnlength = 4;
		colu8.columnposition = 5;
		column.push_back(colu8);

////add to file

		RID ridcreatecolumn;
		FileHandle filehandlecreatecolumn;
		RecordBasedFileManager:: instance()->createFile("Columns");
		RecordBasedFileManager:: instance()->openFile("Columns",filehandlecreatecolumn);



		//
		int size2 = column.size();
		int j =0;
		Columns it2;
	//	cout<<size2<<"size"<<endl;

		for(j=0;j<size2;j++)
		 {
			it2 = column[j];
			void* columnrecord = (void*)malloc(71);
			int nulla = 0;
			memcpy(columnrecord, &nulla, 1);

			void *ID2 = malloc(4);
			memset(ID2, 0, sizeof(int));
			*(int*)ID2 = it2.tableid;
			memcpy((char*)columnrecord + 1, (char*)ID2, 4);
			free(ID2);



			int lengtha = 50;//value is wrong
			memcpy((char*)columnrecord + 5, &lengtha, 4);//size of int
			char* namecolumn = (char*)malloc(50);
			memset(namecolumn, 0, 50);
			memcpy(namecolumn,it2.columnname,50);
			memcpy((char*)columnrecord + 9, (char*)namecolumn, 50);
			free(namecolumn);


			void *type = malloc(4);
			memset(type, 0, sizeof(int));
			*(int*)type = it2.columntype;
			memcpy((char*)columnrecord + 59, (char*)type, 4);
			free(type);

			void *length = malloc(4);
			memset(length, 0, sizeof(int));
			*(int*)length = it2.columnlength;
			memcpy((char*)columnrecord + 63, (char*)length, 4);
			free(length);

			void *position = malloc(4);
			memset(position, 0, sizeof(int));
			*(int*)position = it2.columnposition;
			memcpy((char*)columnrecord + 67,(char*) position, 4);
			free(position);

			RecordBasedFileManager::instance()->insertRecord(filehandlecreatecolumn, recordDescriptorcolumn, columnrecord, ridcreatecolumn);
			free(columnrecord);
		 }
		RecordBasedFileManager::instance()->closeFile(filehandlecreatecolumn);

		return 0;
}

RC RelationManager::deleteCatalog()
{
	remove("Tables");
	remove("Columns");
	return 0;

}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)//???????????/attrs??
{

	unsigned long fileLength;
	char* c;
	const int len = tableName.length();
	c = new char[len+1];
	strcpy(c,tableName.c_str());

	vector<Attribute> recordDescriptor;
	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	recordDescriptor.push_back(attr);
	Attribute attr2;
	attr2.name = "table-name";
	attr2.type = TypeVarChar;
	attr2.length = (AttrLength)50;
	recordDescriptor.push_back(attr2);
	Attribute attr3;
	attr3.name = "file-name";
	attr3.type = TypeVarChar;
	attr3.length = (AttrLength)50;
	recordDescriptor.push_back(attr3);

	RID ridcreate;
	FileHandle filehandlecreate;
	RecordBasedFileManager::instance()->createFile(c);

	RecordBasedFileManager::instance()->openFile("Tables", filehandlecreate);
	void* read_pag=malloc(PAGE_SIZE);
	filehandlecreate.readPage(filehandlecreate.getNumberOfPages()-1,read_pag);////////////////////////////////////bu yanjin!!!!!!!!!!!

	//ge id
	short int slotnum = 0;
	int off_tab = PAGE_SIZE-4;
	memcpy(&slotnum,off_tab+(char*)read_pag,2);
	int numtab = slotnum;





	Tables lists;
	lists.tableid = numtab;//wrong , need read table first!!!!!!!!!!!!!!!!!
	char *str;
	const int p_length = tableName.length();
	str = new char[p_length+1];
	strcpy(str,tableName.c_str());
	memcpy(lists.filename,str,strlen(str)+1);
	memcpy(lists.tablename,str,strlen(str)+1);
	table.push_back(lists);

	//being record combine
	void* tablerecord = (void*)malloc(113);
	memset(tablerecord,0,113);
	int nulla = 0;
	memcpy(tablerecord, &nulla, 1);
	void *ID = malloc(4);

	memset(ID, 0, sizeof(int));
	Tables it;
	*(int*)ID = table[table.size()-1].tableid+1;
	cout<<*(int*)ID<<endl;
	memcpy((char*)tablerecord + 1, (char*)ID, 4);
	free(ID);

	int lengtha = 50;//value is wrong
	memcpy((char*)tablerecord + 5, &lengtha, 4);//size of int
	char* nametable = (char*)malloc(50);
	memset(nametable, 0, 50);
	memcpy(nametable,table[table.size()-1].tablename,50);
	memcpy((char*)tablerecord + 9, (char*)nametable, 50);
	free(nametable);


	int lengthb = 50;
	memcpy((char*)tablerecord + 59, &lengthb, 4);
	char* namefile = (char*)malloc(50);
	memset(namefile, 0, 50);
	memcpy(namefile,table[table.size()-1].filename,50);
	memcpy((char*)tablerecord + 63,(char*) namefile, 50);
	free(namefile);

	char* nametablea = (char*)malloc(50);
	memcpy((char*)nametablea,(char*)tablerecord+9,50);
	//cout<<nametablea<<"nametablea  "<<endl;
	//cout<<"finishaaca"<<nametablea<<endl;
	free(nametablea);

	RecordBasedFileManager::instance()->insertRecord(filehandlecreate, recordDescriptor, tablerecord, ridcreate);
/*
	void*data = malloc(113);
	memset(data,0,113);
	RecordBasedFileManager::instance()->readRecord(filehandlecreate, recordDescriptor, ridcreate, data);
	void*readtable = malloc(50);
	memset(readtable,0,50);
	memcpy(readtable,tablerecord+9,50);
	cout<<readtable<<"readtable"<<endl;
	free(readtable);
	free(data);
	*/
	free(tablerecord);

	vector<Attribute> recordDescriptorcolumn;
	Attribute attrc;
	attrc.name = "table-id";
	attrc.type = TypeInt;
	attrc.length = (AttrLength)4;
	recordDescriptorcolumn.push_back(attrc);

	Attribute attrc2;
	attrc2.name = "column-name";
	attrc2.type = TypeVarChar;
	attrc2.length = (AttrLength)50;
	recordDescriptorcolumn.push_back(attrc2);

	Attribute attrc3;
	attrc3.name = "column-type";
	attrc3.type = TypeInt;
	attrc3.length = (AttrLength)4;
	recordDescriptorcolumn.push_back(attrc3);

	Attribute attrc4;
	attrc4.name = "column-length";
	attrc4.type = TypeInt;
	attrc4.length = (AttrLength)4;
	recordDescriptorcolumn.push_back(attrc4);

	Attribute attrc5;
	attrc5.name = "column-position";
	attrc5.type = TypeInt;
	attrc5.length = (AttrLength)4;
	recordDescriptorcolumn.push_back(attrc5);

	RID ridcreatecolumn;
	FileHandle filehandlecreatecolumn;
	RecordBasedFileManager::instance()->createFile("Columns");
	RecordBasedFileManager::instance()->openFile("Columns", filehandlecreatecolumn);

	void* read_pagcol=malloc(PAGE_SIZE);
	filehandlecreate.readPage(filehandlecreatecolumn.getNumberOfPages()-1,read_pagcol);

	//ge id
	short int slotnumcol = 0;
	int off_col = PAGE_SIZE-4;
	memcpy(&slotnumcol,off_col+(char*)read_pag,2);
	int numcol= slotnum;

	Columns lists2;
	int num =0;
	num = attrs.size();
	int num_col_init = column.size();//before add table
//	cout<<num_col_init<<"initnumcol"<<endl;
	for(int i=0;i<num;i++)
	{
		lists2.tableid = numcol+1;//
		char *str2;
		const int p_lengthc = attrs[i].name.length();
		str2 = new char[p_lengthc+1];
		strcpy(str2,attrs[i].name.c_str());

		memcpy(lists2.columnname,str2,strlen(str2));
//		cout<<lists2.columnname<<"columnname"<<endl;
		lists2.columntype = attrs[i].type;
		lists2.columnlength = attrs[i].length;
		lists2.columnposition = i+1;
		column.push_back(lists2);
		memcpy(lists2.columnname,"                  ",18);
	}
	int num_col_aft = column.size(); //after add table
//	cout<<num_col_aft<<"initnumcol"<<endl;

	for(int j =0;j<=num_col_aft-num_col_init-1;j++)
	{
		void* columnrecord = (void*)malloc(71);
		int nulla = 0;
		memcpy(columnrecord, &nulla, 1);

		void *ID2 = malloc(4);
		memset(ID2, 0, sizeof(int));
		*(int*)ID2 = column[num_col_init+j].tableid;//           change!
		memcpy((char*)columnrecord + 1, (char*)ID2, 4);
		free(ID2);

		int lengtha = 50;//value is wrong
		memcpy((char*)columnrecord + 5, &lengtha, 4);//size of int
		char* namecolumn = (char*)malloc(50);
		memset(namecolumn, 0, 50);
		memcpy(namecolumn,column[num_col_init+j].columnname,50);
		memcpy((char*)columnrecord + 9, (char*)namecolumn, 50);
		free(namecolumn);

		void *type = malloc(4);
		memset(type, 0, sizeof(int));
		*(int*)type = column[num_col_init+j].columntype;
		memcpy((char*)columnrecord + 59, (char*)type, 4);
		free(type);

		void *length = malloc(4);
		memset(length, 0, sizeof(int));
		*(int*)length = column[num_col_init+j].columnlength;
		memcpy((char*)columnrecord + 63, (char*)length, 4);
		free(length);

		void *position = malloc(4);
		memset(position, 0, sizeof(int));
		*(int*)position = column[num_col_init+j].columnposition;
		memcpy((char*)columnrecord + 67, (char*)position, 4);
		free(position);

		RecordBasedFileManager::instance()->insertRecord(filehandlecreatecolumn, recordDescriptorcolumn, columnrecord, ridcreatecolumn);
		free(columnrecord);
	}

	RecordBasedFileManager::instance()->closeFile(filehandlecreatecolumn);
	vector<Tables>().swap(table);
	vector<Columns>().swap(column);//!!!!!!!!!!!!!!!!!!!!!!!!!!
    return 0;

}

RC RelationManager::deleteTable(const string &tableName)
{
	// find the table, get the position, read the attributes

	unsigned long fileLength, fileLength2;
	RID ridcreate;
	FileHandle filehandlecreate;

	RecordBasedFileManager::instance()->openFile("Tables", filehandlecreate);
	void* read_pag = malloc(PAGE_SIZE);
	filehandlecreate.readPage(filehandlecreate.getNumberOfPages() - 1, read_pag);

	short int slotnum = 0;
	int off_tab = PAGE_SIZE - 4;
	memcpy(&slotnum, off_tab + (char*)read_pag, 2);
	int numtab = slotnum;

	int table_pos = 0;

	int* table_posid;
	table_posid = (int*)malloc(4);
	int* col_posid;
	col_posid = (int*)malloc(4);
	Tables lists1;
	char *str;
	const int p_length = tableName.length();
	str = new char[p_length + 1];
	strcpy(str, tableName.c_str());
	memcpy(lists1.filename, str, strlen(str));
	Tables lists2table;
	memcpy(lists2table.filename, str, strlen(str));

	if(tableName=="Tables"||tableName=="Columns")
	{
	//	cout<<"should not delete"<<endl;
		return -1;
	}

	int slot_all;
	RID ridtab;
	vector<Attribute> recordDescriptora;
	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	recordDescriptora.push_back(attr);
	Attribute attr2;
	attr2.name = "table-name";
	attr2.type = TypeVarChar;
	attr2.length = (AttrLength)50;
	recordDescriptora.push_back(attr2);
	Attribute attr3;
	attr3.name = "file-name";
	attr3.type = TypeVarChar;
	attr3.length = (AttrLength)50;
	recordDescriptora.push_back(attr3);
	unsigned n = 1;

	for (int pag = 0;pag<filehandlecreate.getNumberOfPages();pag++)
	{

		void* pag_slot = malloc(PAGE_SIZE);
		memset(pag_slot, 0, PAGE_SIZE);
		filehandlecreate.readPage(pag, pag_slot);

		memcpy(&slot_all, off_tab + (char*)pag_slot, 2);
		free(pag_slot);
		for (int slot = 1;slot <= slot_all;slot++)
		{

			//		cout<<"slot all"<<slot_all<<endl;//why slot all ==8?
			ridtab.pageNum = pag;
			ridtab.slotNum = slot;

			void*data = malloc(113);
			memset(data, 0, 113);
			//		cout<<"recordDescriptorasssss before read::"<<recordDescriptora[2].type<<endl;

			RecordBasedFileManager::instance()->readRecord(filehandlecreate, recordDescriptora, ridtab, data);
			//		cout<<"recordDescriptorasssss::after read::"<<recordDescriptora[2].type<<endl;
			char*readtable = (char*)malloc(50);
			memset(readtable, 0, 50);
			memcpy(readtable, (char*)data + 9, 50);

			std::string ss(lists1.filename);
			ss.erase(ss.find_last_not_of(" ") + 1);

			std::string ssreadtale = readtable;
			//ssreadtable.erase(ssreadtable.find_last_not_of(" ") + 1);
			//			cout<<ssreadtale<<"ssreadtaleaaaa"<<endl;
	//				cout<<readtable<<"readtableaaa"<<endl;
	//				cout<<lists2table.filename<<"lists2table.filename2121"<<endl;
	//				cout<<ss<<"lists1.filename"<<endl;
			if (ss == readtable || ssreadtale == lists2table.filename)
			{
				//	table_post=*table_posid+1;  // CHANGE  //table id 1,2,3,4 if deliete 3, 1,2,4,
				//only 104 bytes the position of 4 is 3,use position to delete,so get former id and add 1
				//			cout<<"enter compare"<<endl;
				int idtab = 0;
				memcpy(&idtab, (char*)data + 1, 4);
				table_pos = idtab;
	//			cout<<"begin delete"<<endl;
				RecordBasedFileManager::instance()->deleteRecord(filehandlecreate, recordDescriptora, ridtab);
	//			cout << table_pos << "positions" << endl;
				break;
			}
			else
			{
				table_pos = 1;
			}
			free(data);
			free(readtable);

		}
	//	cout << "end for" << endl;
		if (n == 0)
		{
			break;
		}
	}

	RecordBasedFileManager::instance()->closeFile(filehandlecreate);

    //////////////////////////////////delete  column


    RID ridcol;
	FileHandle filehandlecreatecolumn;
	RecordBasedFileManager::instance()->openFile("Columns", filehandlecreatecolumn);
	void* read_pagcol = malloc(PAGE_SIZE);
	//	cout<<"enter111    "<<filehandlecreatecolumn.getNumberOfPages()<<endl;
	filehandlecreatecolumn.readPage(0, read_pagcol);


	//ge id
	short int slotnumcol = 0;
	int off_tabcol = PAGE_SIZE - 4;
	memcpy(&slotnumcol, off_tab + (char*)read_pagcol, 2);
	int numcol = slotnumcol;

	Columns lists2;
	vector<Attribute> recordDescriptorcolumna;
	Attribute attrc;
	attrc.name = "table-id";
	attrc.type = TypeInt;
	attrc.length = (AttrLength)4;
	recordDescriptorcolumna.push_back(attrc);

	Attribute attrc2;
	attrc2.name = "column-name";
	attrc2.type = TypeVarChar;
	attrc2.length = (AttrLength)50;
	recordDescriptorcolumna.push_back(attrc2);

	Attribute attrc3;
	attrc3.name = "column-type";
	attrc3.type = TypeInt;
	attrc3.length = (AttrLength)4;
	recordDescriptorcolumna.push_back(attrc3);

	Attribute attrc4;
	attrc4.name = "column-length";
	attrc4.type = TypeInt;
	attrc4.length = (AttrLength)4;
	recordDescriptorcolumna.push_back(attrc4);

	Attribute attrc5;
	attrc5.name = "column-position";
	attrc5.type = TypeInt;
	attrc5.length = (AttrLength)4;
	recordDescriptorcolumna.push_back(attrc5);

	lists2.columnposition = table_pos;
	int slot_all_col = 0;

	int nnncolnum = filehandlecreatecolumn.getNumberOfPages();

	for (int pagcol = 0;pagcol<nnncolnum;pagcol++)
	{
		void* pag_slot = malloc(PAGE_SIZE);
		memset(pag_slot, 0, PAGE_SIZE);
		filehandlecreatecolumn.readPage(pagcol, pag_slot);
		memcpy(&slot_all_col, off_tab + (char*)pag_slot, 2);
		free(pag_slot);
		int asa = 0;
		int asaa = 0;
		for (int slot = 1;slot <= slot_all_col;slot++)
		{
			ridcol.pageNum = pagcol;
			ridcol.slotNum = slot;
			//	cout<<"slot all col"<<slot_all_col<<"slotcurrent"<<ridcol.slotNum<<endl;
			void*data2 = malloc(71);
			memset(data2, 0, 71);
			RecordBasedFileManager::instance()->readRecord(filehandlecreatecolumn, recordDescriptorcolumna, ridcol, data2);
			int*readtable = (int*)malloc(4);
			memset(readtable, 0, 4);
			memcpy(readtable, (char*)data2 + 1, 4);

			if (*readtable == lists2.columnposition)
			{
				asaa= asaa+1;
	//			cout<<"begin to delete column"<<endl;
	//			cout<<"delete i times i=  "<<asaa<<endl;
				RecordBasedFileManager::instance()->deleteRecord(filehandlecreatecolumn,recordDescriptorcolumna,ridcol);
			}
			free(data2);
			free(readtable);
		}
	}
	 RecordBasedFileManager::instance()->closeFile(filehandlecreatecolumn);
	remove(str);//delete file
	//////////////////////////////

	return 0;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	if(IndexManager::instance()->createFile("index_"+tableName+"_"+attributeName)!=0)
	{
		return -1;
	}
	FileHandle fileHandle;
	IXFileHandle fileHandle1;
		RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
		rbfm->openFile(tableName,fileHandle);
		IndexManager::instance()->openFile("index_"+tableName+"_"+attributeName,fileHandle1);
		vector <string> x;
		x.push_back(attributeName);
		vector <Attribute> recordDescriptor;
		getAttributes(tableName,recordDescriptor);
		RBFM_ScanIterator rbfm_scanIterator;
		rbfm->scan(fileHandle,recordDescriptor,"",NO_OP,NULL,x,rbfm_scanIterator);
		void *data=malloc(PAGE_SIZE);
		RID rid;
		int ind=0;
		for(ind=0;ind<recordDescriptor.size();ind++)
			if(recordDescriptor[ind].name==attributeName)
				break;
		while(rbfm_scanIterator.getNextRecord(rid,data)!=-1)
		{
			IndexManager::instance()->insertEntry(fileHandle1,recordDescriptor[ind],data,rid);
		}
		IndexManager::instance()->closeFile(fileHandle1);
		rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager:: destroyIndex(const string &tableName, const string &attributeName)
{
	//if(!init)
	//	initialize();
	IndexManager *ix=IndexManager::instance();
	return (ix->destroyFile("index_"+tableName+"_"+attributeName));
}

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
					const string &attributeName,
					const void *lowKey,
					const void *highKey,
					bool lowKeyInclusive,
					bool highKeyInclusive,
					RM_IndexScanIterator &rm_IndexScanIterator
   )
{
	IndexManager *ix=IndexManager::instance();
		IXFileHandle fileHandle;
		if(IndexManager::instance()->openFile("index_"+tableName+"_"+attributeName,fileHandle)!=0)
			return -1;
		IX_ScanIterator ix_scanIterator;
		vector <Attribute> recordDescriptor;
		getAttributes(tableName,recordDescriptor);
		int ind=0;
		for(ind=0;ind<recordDescriptor.size();ind++)
			if(recordDescriptor[ind].name==attributeName)
				break;
		if(ind==recordDescriptor.size())
			return -1;
		ix->scan(fileHandle,recordDescriptor[ind],lowKey,highKey,lowKeyInclusive,highKeyInclusive,ix_scanIterator);
		rm_IndexScanIterator.ix_scanIterator=ix_scanIterator;

		cout<<"xxxxxx"<<ix_scanIterator.pagenum<<endl;
  return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	// find the table, get the position, read the attributes

		unsigned long fileLength,fileLength2;
		RID ridcreate;
		FileHandle filehandlecreate;

		RecordBasedFileManager::instance()->openFile("Tables", filehandlecreate);
		void* read_pag = malloc(PAGE_SIZE);
		filehandlecreate.readPage(filehandlecreate.getNumberOfPages() - 1, read_pag);

		short int slotnum = 0;
		int off_tab = PAGE_SIZE - 4;
		memcpy(&slotnum, off_tab + (char*)read_pag, 2);
		int numtab = slotnum;

		int table_pos=0;

		int* table_posid;
		table_posid = (int*)malloc(4);
		int* col_posid;
		col_posid = (int*)malloc(4);
		Tables lists1;
		char *str;
		const int p_length = tableName.length();
		str = new char[p_length+1];
		strcpy(str,tableName.c_str());
		memcpy(lists1.filename,str,strlen(str));
		Tables lists2table;
		memcpy(lists2table.filename,str,strlen(str));

		int slot_all;
		RID ridtab;
		vector<Attribute> recordDescriptora;
		Attribute attr;
		attr.name = "table-id";
		attr.type = TypeInt;
		attr.length = (AttrLength)4;
		recordDescriptora.push_back(attr);
		Attribute attr2;
		attr2.name = "table-name";
		attr2.type = TypeVarChar;
		attr2.length = (AttrLength)50;
		recordDescriptora.push_back(attr2);
		Attribute attr3;
		attr3.name = "file-name";
		attr3.type = TypeVarChar;
		attr3.length = (AttrLength)50;
		recordDescriptora.push_back(attr3);
		unsigned n =1;

		for(int pag=0;pag<filehandlecreate.getNumberOfPages();pag++)
		{

			void* pag_slot = malloc(PAGE_SIZE);
			memset(pag_slot,0,PAGE_SIZE);
			filehandlecreate.readPage(pag, pag_slot);

			memcpy(&slot_all, off_tab + (char*)pag_slot, 2);
			free(pag_slot);
			for(int slot =1;slot<=slot_all;slot++)
			{

		//		cout<<"slot all"<<slot_all<<endl;//why slot all ==8?
				ridtab.pageNum = pag;
				ridtab.slotNum = slot;

				void*data = malloc(113);
				memset(data,0,113);
		//		cout<<"recordDescriptorasssss before read::"<<recordDescriptora[2].type<<endl;

				RecordBasedFileManager::instance()->readRecord(filehandlecreate, recordDescriptora, ridtab, data);
		//		cout<<"recordDescriptorasssss::after read::"<<recordDescriptora[2].type<<endl;
				char*readtable = (char*)malloc(50);
				memset(readtable,0,50);
				memcpy(readtable,(char*)data+9,50);

				std::string ss(lists1.filename);
				ss.erase(ss.find_last_not_of(" ") + 1);

				std::string ssreadtale= readtable;
				//ssreadtable.erase(ssreadtable.find_last_not_of(" ") + 1);
	//			cout<<ssreadtale<<"ssreadtaleaaaa"<<endl;
		//		cout<<readtable<<"readtableaaa"<<endl;
		//		cout<<lists2table.filename<<"lists2table.filename2121"<<endl;
		//		cout<<ss<<"lists1.filename"<<endl;
				if(ss==readtable||ssreadtale==lists2table.filename)
				{
				//	table_post=*table_posid+1;  // CHANGE  //table id 1,2,3,4 if deliete 3, 1,2,4,
					//only 104 bytes the position of 4 is 3,use position to delete,so get former id and add 1
		//			cout<<"enter compare"<<endl;
					int idtab=0;
					memcpy(&idtab,(char*)data+1,4);
					//we use true col id to delete, so we need find the excate id
	//				cout<<*col_posid<<"TABLEIDDDD"<<endl;
					table_pos =idtab;
	//				cout<<table_pos<<"positions"<<endl;
					break;
				}
				else
				{
					table_pos =1;
				}
				free(data);
				free(readtable);

			}
	//		cout<<"end for"<<endl;
			if(n==0)
			{
				break;
			}
		}
		//all add into #################################
	//is0,1,2,3,... should add 1 ->position
		RecordBasedFileManager::instance()->closeFile(filehandlecreate);
	//	cout<<"finish"<<endl;


	RID ridcol;
	FileHandle filehandlecreatecolumn;
	RecordBasedFileManager::instance()->openFile("Columns", filehandlecreatecolumn);
	void* read_pagcol = malloc(PAGE_SIZE);
//	cout<<"enter111    "<<filehandlecreatecolumn.getNumberOfPages()<<endl;
	filehandlecreatecolumn.readPage(0, read_pagcol);


	//ge id
	short int slotnumcol = 0;
	int off_tabcol = PAGE_SIZE - 4;
	memcpy(&slotnum, off_tab + (char*)read_pagcol, 2);
	int numcol = slotnumcol;


	Columns lists2;
	vector<Attribute> recordDescriptorcolumna;
		Attribute attrc;
		attrc.name = "table-id";
		attrc.type = TypeInt;
		attrc.length = (AttrLength)4;
		recordDescriptorcolumna.push_back(attrc);

		Attribute attrc2;
		attrc2.name = "column-name";
		attrc2.type = TypeVarChar;
		attrc2.length = (AttrLength)50;
		recordDescriptorcolumna.push_back(attrc2);

		Attribute attrc3;
		attrc3.name = "column-type";
		attrc3.type = TypeInt;
		attrc3.length = (AttrLength)4;
		recordDescriptorcolumna.push_back(attrc3);

		Attribute attrc4;
		attrc4.name = "column-length";
		attrc4.type = TypeInt;
		attrc4.length = (AttrLength)4;
		recordDescriptorcolumna.push_back(attrc4);

		Attribute attrc5;
		attrc5.name = "column-position";
		attrc5.type = TypeInt;
		attrc5.length = (AttrLength)4;
		recordDescriptorcolumna.push_back(attrc5);

	lists2.columnposition = table_pos;
	//cout<<lists2.columnposition<<"colupos"<<endl;
	unsigned long col_pos_num=0;
	unsigned long id_before = 0;
	int *  col_id_att;
	col_id_att=(int* ) malloc(4);
	int nnncolnum = filehandlecreatecolumn.getNumberOfPages();
//	cout<<"SIZE PAG"<<nnncolnum<<endl;

	int slot_all_col =0;



	for(int pagcol=0;pagcol<nnncolnum;pagcol++)
			{
				void* pag_slot = malloc(PAGE_SIZE);
				memset(pag_slot,0,PAGE_SIZE);
				filehandlecreatecolumn.readPage(pagcol, pag_slot);
				memcpy(&slot_all_col, off_tab + (char*)pag_slot, 2);
				free(pag_slot);
				int asa =0;
				for(int slot =1;slot<=slot_all_col;slot++)
				{
					ridcol.pageNum = pagcol;
					ridcol.slotNum = slot;
				//	cout<<"slot all col"<<slot_all_col<<"slotcurrent"<<ridcol.slotNum<<endl;
					void*data = malloc(71);
					memset(data,0,71);
					RecordBasedFileManager::instance()->readRecord(filehandlecreatecolumn, recordDescriptorcolumna, ridcol, data);
					int*readtable = (int*)malloc(4);
					memset(readtable,0,4);
					memcpy(readtable,(char*)data+1,4);
	//			cout<<"enter1112"<<endl;
				//	cout<<"enter1113"<<endl;

					if(*readtable==lists2.columnposition)
					{
						asa = asa+1;
					//	cout<<"readtable"<<*readtable<<endl;
					//	cout<<"lists2.columnposition"<<lists2.columnposition<<endl;
					//	cout<<"enter111     "<<asa<<endl;
						Attribute attcol;
						int typa=0;
						memcpy(&typa,(char*)data+59,4);
						switch(typa)
							{
								case  0:
									attcol.type = TypeInt;
									break;
								case 1 :
									attcol.type = TypeReal;
									break;
								case 2:
									attcol.type = TypeVarChar;
									break;
								default :
									return -1;
							}
						int lencol =0;
						memcpy(&lencol,(char*)data+63,4);
						attcol.length = lencol;
						char* chacol = (char*)malloc(50);
						memcpy(chacol,(char*)data+9,50);
						std::string s(chacol);
	//					cout << s << "COLCHARR" << endl;
						s.erase(s.find_last_not_of(" ") + 1);
						attcol.name = s;
						attrs.push_back(attcol);
		//				cout<<"attrs type"<<attrs[asa-1].length<<endl;
					}
					free(data);
					free(readtable);
				}
			}
//	cout<<"finish"<<endl;
//	cout<<"attrs size"<<attrs.size()<<endl;
//	cout<<attrs[0].name<<"attrs[0].name"<<endl;
//	cout<<attrs[1].name<<"attrs[0].name"<<endl;
//	cout<<attrs[2].name<<"attrs[0].name"<<endl;
	RecordBasedFileManager::instance()->closeFile(filehandlecreatecolumn);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle)!=0) return -1;;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    RC rc=RecordBasedFileManager::instance()->insertRecord(fileHandle, recordDescriptor, data, rid);
	RecordBasedFileManager::instance()->closeFile(fileHandle);
    return rc;





}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	FileHandle fileHandle;
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	RecordBasedFileManager::instance()->deleteRecord(fileHandle, recordDescriptor, rid);
	RecordBasedFileManager::instance()->closeFile(fileHandle);
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	FileHandle fileHandle;
	RecordBasedFileManager::instance()->openFile(tableName,fileHandle);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	RecordBasedFileManager::instance()->updateRecord(fileHandle,recordDescriptor,data,rid);
	RecordBasedFileManager::instance()->closeFile(fileHandle);



    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
//	cout<<"----------------------------------------------------------------------------read tuple"<<endl;
//	cout<<"----------------------------------------------------------------------------get attribute"<<endl;
	vector<Attribute> recordDescriptor2;
	getAttributes(tableName, recordDescriptor2);
//	cout<<"----------------------------------------------------------------------------finish get attribute"<<endl;
	int a =rid.slotNum;
//	cout<<"ridslotnum"<<a<<endl;
//	cout<<"ridpagnum"<<rid.pageNum<<endl;
	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle)!=0) return -1;

//	cout<<"tuple in size"<<recordDescriptor2.size()<<endl;

	RC rc = RecordBasedFileManager::instance()->readRecord(fileHandle, recordDescriptor2,rid,data);
	RecordBasedFileManager::instance()->closeFile(fileHandle);
    return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager::instance()->printRecord(attrs,data);
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	RC rc = RecordBasedFileManager::instance()->readAttribute(fileHandle,recordDescriptor,rid,attributeName,data);
	RecordBasedFileManager::instance()->closeFile(fileHandle);
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
//	cout<<"scan                      ------------top value"<<conditionAttribute<<endl;
	FileHandle fileHandle;
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	RecordBasedFileManager::instance()->scan(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_iterator);//wrong);

	rm_ScanIterator.fileHandlescaniterRM = fileHandle;

//cout<<" scan"<<endl;

    return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}
/*
RM_ScanIterator::RM_ScanIterator()
{

}
RM_ScanIterator::~RM_ScanIterator()
{

}
*/
RC RM_ScanIterator:: getNextTuple(RID &rid, void *data)
{

	int a = rbfm_iterator.getNextRecord(rid,data);
	return a;
}

RC RM_ScanIterator:: close()
{
	PagedFileManager::instance()->closeFile(fileHandlescaniterRM);
	return 0;
}



RM_IndexScanIterator::RM_IndexScanIterator()
{
	}

RM_IndexScanIterator::~RM_IndexScanIterator()
{
	}
RC RM_IndexScanIterator:: getNextEntry(RID &rid, void *key)
{
	return ix_scanIterator.getNextEntry(rid,key);
}

RC RM_IndexScanIterator::close()
{
	return -1;//terminate scan the index break up...
}
