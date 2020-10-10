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

#ifndef DPN_H_
#define DPN_H_

#include <boost/shared_ptr.hpp>

#include "common/CommonDefinitions.h"
#include "common/bhassert.h"
#include "core/tools.h"
#include "core/RCAttrPack.h"
#include "fwd.h"

const static char PACK_MODE_TRIVIAL		= 0; //trivial data: all values are derivable from the statistics, or nulls only,
const static char PACK_MODE_UNLOADED	= 1; //on disc
const static char PACK_MODE_IN_MEMORY	= 2; //loaded to memory
const static char PACK_MODE_EMPTY		= 3; //No data yet, empty pack

const static int PF_NULLS_ONLY	= -1; // Special value pack_file=-1 indicates "nulls only".
const static int PF_NO_OBJ		= -2; // Special value pack_file=-2 indicates "no objects".
const static int PF_NOT_KNOWN	= -3; // Special value pack_file=-3 indicates "number not determined yet".
const static int PF_END			= -4; // Special value pack_file=-4 indicates the end of numbers list.
const static int PF_DELETED		= -5; // Special value pack_file=-5 indicates that the datapack has been deleted


struct DPN
{
public:
	std::auto_ptr<DPN> CloneWithoutPack() const;

	DPN( void )
	:	local_min( 0 ),
		local_max( 0 ),
		sum_size( 0 ),
		pack_file( PF_NO_OBJ ),
		pack_addr( 0 ),
		no_nulls( 0 ),
		no_objs( 0 ),
		no_pack_locks(0),
		pack(),
		pack_mode( PACK_MODE_EMPTY ),
		is_stored( false ),
		repetition_found( false ),
		natural_save_size( 0 )
	{}
	~DPN() {}
	AttrPackS *GetPackS() const;
	AttrPackN *GetPackN() const;
	AttrPack* GetPack() const;
	bool IsNull(int obj) const;

	uint GetNoObj() const { return pack_mode == PACK_MODE_EMPTY || pack_file == PF_NO_OBJ ? 0 : (uint)no_objs + 1; }
// ------------------------------------------------------------
	_int64	local_min;
	_int64	local_max;
	_int64	sum_size;
	int		pack_file;
	uint	pack_addr;
	uint	no_nulls;
	ushort	no_objs;
	// for whole pack
	ushort	no_pack_locks;
	AttrPackPtr pack;
	//如果已经有完整的Pack包,则不会再出现range pack
	// TODO: 按条件检索时,	进一步优化,可以考虑放弃完整数据包,仍然使用检索条件到riak查询
	// for range pack
	struct PackR {
	    PackR():no_pack_locks(0),pack(){};
		~PackR() {pack.reset();}
		ushort no_pack_locks;
		AttrPackPtr pack;
	};
	struct sub_packs{
		// range+queryhash=map key 
		std::map<ulong,PackR> packs;
		//transaction related pack range
		std::map<ulong,ulong> packtrans;
		void LinkTrans(ulong dkey,ulong sid) {
			packtrans[sid]=dkey;
		}
		void PutSubPack(ulong dkey,AttrPackPtr &p) {
			packs[dkey].pack = p;
			packs[dkey].no_pack_locks++;
		}
		void ResetSubPack() {
			packs.clear();
			packtrans.clear();
		}

		bool SubPackEmpty() {
		 	return packs.size()==0;
		}

		void VisitSubPack(ulong dkey) {
			packs[dkey].no_pack_locks++;
		}

		void LockSubPack(ulong dkey,ulong sid) {
			packs[dkey].no_pack_locks++;
			packtrans[sid]=dkey;
			packs[dkey].pack->Lock();
		}
		bool HasSubPack(ulong dkey) {
			return packs.count(dkey)>0;
		}
		// unlock range pack only
		bool UnlockSubPack() ;
	};

	boost::shared_ptr<sub_packs> subpackptr;

	void LinkTrans(ulong dkey,ulong sid) {
		if(subpackptr.get()==NULL) return;
		subpackptr->LinkTrans(dkey,sid);
	}

	void PutSubPack(ulong dkey,AttrPackPtr &p) {
		if(subpackptr.get()==NULL)
			subpackptr=boost::shared_ptr<sub_packs>(new sub_packs());
		subpackptr->PutSubPack(dkey,p);
	}
	
	void ResetSubPack() {
		if(subpackptr.get()!=NULL)
			subpackptr->ResetSubPack();
		subpackptr.reset();
	}

	bool SubPackEmpty() {
		if(subpackptr.get()==NULL) return true;
		return subpackptr->SubPackEmpty();
	}
	
	void VisitSubPack(ulong dkey) {
		subpackptr->VisitSubPack(dkey);
	}

	void LockSubPack(ulong dkey,ulong sid) {
		subpackptr->LockSubPack(dkey,sid);
	}

	bool UnlockSubPack() {
		if(subpackptr.get()==NULL) return false;
		return subpackptr->UnlockSubPack();
	}
	
	bool HasSubPack(ulong dkey) {
		if(subpackptr.get()==NULL) return false;
		return subpackptr->HasSubPack(dkey);
	}

	char	pack_mode;
	bool	is_stored;
	bool	repetition_found;
	uint	natural_save_size;
	void CopyToBuffer(char* buf, AttrPackType packtype);
	void RestoreFromBuffer(char* buf, AttrPackType packtype);
	int64 GetRiakKey() {
		assert(pack_file>=0);
		return (((int64)pack_file)<<32)+pack_addr;
	}
public:
	static const int STORED_SIZE = 37;
};

template<>
struct copy_object<DPN>
{
	DPN operator()( DPN const& obj ) {
		DPN copy( obj );
		//copy.pack.reset();
		//copy.no_pack_locks = 0;
		//copy.packs.clear();
		return ( copy );
	}
};

#endif /*DPN_H_*/
