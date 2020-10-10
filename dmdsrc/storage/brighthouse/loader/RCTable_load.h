/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#ifndef RCTABLE_LOAD_H_
#define RCTABLE_LOAD_H_

#include "core/RCTableImpl.h"

class Buffer;
class IOParameters;

class RCTableLoad : public RCTableImpl
{
public:
	RCTableLoad(std::string const& path, int current_state, std::vector<DTCollation> charsets = std::vector<DTCollation>() ) throw(DatabaseRCException);
	RCTableLoad(int na) : RCTableImpl(na) {}
	~RCTableLoad();
	// connect to a table (on disk) identified by number;
	// connection mode:
	// 0 - read only (queries),
	// 1 - write session (s_id - session identifier).
	//     The first use of 1 with a new s_id opens a new session
	void LoadPack(int n);

	// for insert rows only
	void PrepareLoadData(uint connid);	
	void LoadData(IOParameters& iop,uint connid);
	void LoadData(IOParameters& iop, Buffer& buffer);
	void LoadData(uint connid,NewValuesSetBase **nvs);
	// for insert only
	void LoadDataEnd();
	
	void WaitForSaveThreads();
	void LoadAttribute(int attr_no,bool loadapindex = true);
	_int64 NoRecordsLoaded();
private:
	_uint64 no_loaded_rows;
};

typedef std::auto_ptr<RCTableLoad> RCTableLoadAutoPtr;

#endif /*RCTABLE_LOAD_H_*/
