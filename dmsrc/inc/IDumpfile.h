/* idumpfile.h --

This file is the interface of dump data from files

Copyright (C) 2012 DataComo
All Rights Reserved.

liujianshun
<liujianshun@datacomo.com>
http://www.datacomo.com
http://mobi5.cn

JIRA : DM-201
*/
#ifndef IDUMPFILE_DEF_H
#define IDUMPFILE_DEF_H

/*
    IFileParser : interface of the FileParser
*/
class SysAdmin;
class IDumpFileWrapper;

class IFileParser
{
public:
    IFileParser() {};
    virtual ~IFileParser() {};
public:
    virtual int GetErrors() = 0;
    virtual int WriteErrorData(const char *line) = 0;
    // set table ,partition,IDumpFileWrapper info, call this function following the construct function
    virtual void SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw) = 0;
    // get file from local or ftp server
    virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) = 0 ;
    virtual bool ExtractEnd(int _dbc) = 0;
    virtual int GetIdleSeconds()= 0 ;
    // do parse the file,filename is seted by function:GetFile
    virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) = 0 ;
    virtual int GetFileSeq(const char *filename) = 0;
    virtual void PluginPrintVersion() = 0;
    virtual void SetSession(const int slaverIndex=0) = 0;
    virtual int GetForceEndDump() = 0;

    // 插件是否整表作为统一单位进行一起采集数据
    // 1> [default: false]单个分区采集数据
    // 2> [true]整表采集数据,解决DMA-1448问题,通过[files::dumpfileforholetable]进行获取
    virtual bool GetDumpfileForHoleTable() = 0; 
};

/*
    IDumpFileWrapper : interface of the DumpFileWrapper
*/
class IDumpFileWrapper
{
public:
    IDumpFileWrapper() {};
    virtual ~IDumpFileWrapper() {};
public:
    /* get file parser  */
    virtual IFileParser *getParser() = 0;

    /* get control file item info */
    virtual const char *getOption(const char *keyval,const char *defaults) = 0 ;
    virtual const char getOption(const char *keyval,const char defaults) =0;
    virtual int getOption(const char *keyval,int defaults)=0 ;
    virtual double getOption(const char *keyval,double defaults) = 0;
};

#endif

