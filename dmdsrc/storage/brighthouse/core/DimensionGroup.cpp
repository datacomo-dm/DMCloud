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

#include "DimensionGroup.h"
#include <vector>

using namespace std;

/////////////////////////////////// DimensionVector //////////////////////////////

DimensionVector::DimensionVector() : size(0), v(NULL)
{}

DimensionVector::DimensionVector(int no_dims) : v(NULL)
{
	size = no_dims;
	if(size > 0)
		v = new bool [size];
	for(int i = 0; i < size; i++)
		v[i] = false;
}

DimensionVector::DimensionVector(const DimensionVector &sec) : v(NULL)
{
	size = sec.size;
	if(size > 0)
		v = new bool [size];
	for(int i = 0; i < size; i++)
		v[i] = sec.v[i];
}

DimensionVector::~DimensionVector()
{
	delete [] v;
}

void DimensionVector::Resize(int no_dims)
{
	if(no_dims <= size)
		return;
	if(no_dims > 0) {
		bool *new_v = new bool [no_dims];
		for(int i = 0; i < size; i++)
			new_v[i] = v[i];
		for(int i = size; i < no_dims; i++)
			new_v[i] = false;
		delete [] v;
		v = new_v;
		size = no_dims;
	}
}

DimensionVector &DimensionVector::operator=(const DimensionVector &sec)
{
	if(&sec != this) {
		if(size != sec.size) {
			size = sec.size;
			delete [] v;
			v = (size > 0) ? new bool [size] : NULL;
		}
		for(int i = 0; i < size; i++)
			v[i] = sec.v[i];
	}
	return *this;
}

bool DimensionVector::operator==(const DimensionVector &d2) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(Size() == d2.Size());
	for(int i = 0; i< Size(); ++i)
		if(v[i] != d2.v[i])
			return false;
	return true;
}

void DimensionVector::Clean()
{
	for(int i = 0; i < size; i++)
		v[i] = false;
}

void DimensionVector::SetAll()
{
	for(int i = 0; i < size; i++)
		v[i] = true;
}

void DimensionVector::Complement()
{
	for(int i = 0; i < size; i++)
		v[i] = !v[i];
}

bool DimensionVector::Intersects(DimensionVector &sec)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(size == sec.size);
	for(int i = 0; i < size; i++)
		if(v[i] && sec.v[i])
			return true;
	return false;
}

void DimensionVector::Minus(DimensionVector &sec)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(size == sec.size);
	for(int i = 0; i < size; i++)
		if(sec.v[i])
			v[i] = false;
}

void DimensionVector::Plus(DimensionVector &sec)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(size == sec.size);
	for(int i = 0; i < size; i++)
		if(sec.v[i])
			v[i] = true;
}

bool DimensionVector::Includes(DimensionVector &sec)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(size == sec.size);
	for(int i = 0; i < size; i++)
		if(sec.v[i] && !v[i])
			return false;
	return true;
}

bool DimensionVector::IsEmpty() const
{
	for(int i = 0; i < size; i++)
		if(v[i])
			return false;
	return true;
}

int DimensionVector::NoDimsUsed() const						// return a number of present dimensions
{
	int res = 0;
	for(int i = 0; i < size; i++)
		if(v[i])
			res++;
	return res;
}

int DimensionVector::GetOneDim()
{
	int res = -1;
	for(int i = 0; i < size; i++)
		if(v[i]) {
			if(res != -1)
				return -1;
			res = i;
		}
	return res;
}

//////////////////////////////////////////////////////////////////////////////////

DimensionGroupFilter::DimensionGroupFilter(int dim, _int64 size,std::map<int,int> *pnfblocks)
{
	base_dim = dim;
	std::map<int,int> nfblocks;
	f = new Filter(size, pnfblocks!=NULL?*pnfblocks:nfblocks,true);		// all ones
	dim_group_type = DG_FILTER;
	no_obj = size;
}

DimensionGroupFilter::DimensionGroupFilter(int dim, Filter *f_source, int copy_mode)	// copy_mode: 0 - copy filter, 1 - ShallowCopy filter, 2 - grab pointer
{
	base_dim = dim;
	f = NULL;
	if(copy_mode == 0)
		f = new Filter(*f_source);
	else if(copy_mode == 1)
		f = Filter::ShallowCopy(*f_source);
	else if(copy_mode == 2)
		f = f_source;			
	dim_group_type = DG_FILTER;
	no_obj = f->NoOnes();
}

DimensionGroupFilter::~DimensionGroupFilter()
{
	delete f;
}

DimensionGroup::Iterator *DimensionGroupFilter::NewIterator(DimensionVector& dim)
{
	assert(dim[base_dim]);
	return new DGFilterIterator(f);
}

DimensionGroup::Iterator* DimensionGroupFilter::NewOrderedIterator(DimensionVector& dim, PackOrderer *po)
{
	assert(dim[base_dim]);
	return new DGFilterOrderedIterator(f, po);
}

DimensionGroupFilter::DGFilterIterator::DGFilterIterator(const Iterator& sec) : DimensionGroup::Iterator(sec)
{
	DGFilterIterator* s = (DGFilterIterator*)(&sec);
	fi = s->fi;
	f = s->f;
}

DimensionGroupFilter::DGFilterOrderedIterator::DGFilterOrderedIterator(const Iterator& sec) : DimensionGroup::Iterator(sec)
{
	DGFilterOrderedIterator* s = (DGFilterOrderedIterator*)(&sec);
	fi = s->fi;
	f = s->f;
}

DimensionGroup::Iterator* DimensionGroupFilter::CopyIterator(DimensionGroup::Iterator* s)
{ 
	DGFilterIterator* sfit = (DGFilterIterator*)s;
	if(sfit->Ordered())
		return new DGFilterOrderedIterator(*s); 
	return new DGFilterIterator(*s); 
}

//////////////////////////////////////////////////////////////////////////////////

DimensionGroupMaterialized::DimensionGroupMaterialized(DimensionVector &dims)
{
	dim_group_type = DG_INDEX_TABLE;
	dims_used = dims;
	no_dims = dims.Size();
	no_obj = 0;
	t = new IndexTable * [no_dims];
	nulls_possible = new bool [no_dims];
	for(int i = 0; i < no_dims; i++) {
		t[i] = NULL;
		nulls_possible[i] = false;
	}
}

DimensionGroup* DimensionGroupMaterialized::Clone(bool shallow)
{
	DimensionGroupMaterialized *new_value = new DimensionGroupMaterialized(dims_used);
	new_value->no_obj = no_obj;
	if(shallow)
		return new_value;
	for(int i = 0; i < no_dims; i++) {
		if(t[i]) {
			new_value->nulls_possible[i] = nulls_possible[i];
			t[i]->Lock();
			new_value->t[i] = new IndexTable(*t[i]);
			t[i]->Unlock();
		}
	}
	return new_value;
}

DimensionGroupMaterialized::~DimensionGroupMaterialized()
{
	for(int i = 0; i < no_dims; i++)
		delete t[i];
	delete [] t;
	delete [] nulls_possible;
}

void DimensionGroupMaterialized::Empty()
{
	for(int i = 0; i < no_dims; i++) {
		delete t[i];
		t[i] = NULL;
	}
	no_obj = 0;
}

void DimensionGroupMaterialized::NewDimensionContent(int dim, IndexTable *tnew, bool nulls)		// tnew will be added (as a pointer to be deleted by destructor) on a dimension dim
{
	assert(dims_used[dim]);
	delete t[dim];
	t[dim] = tnew;
	nulls_possible[dim] = nulls;
}

void DimensionGroupMaterialized::FillCurrentPos(DimensionGroup::Iterator *it, _int64 *cur_pos, int *cur_pack, DimensionVector &dims)
{
	for(int d = 0; d < no_dims; d++)
		if(dims[d] && t[d]) {
			cur_pos[d] = it->GetCurPos(d);
			cur_pack[d] = it->GetCurPackrow(d);
		}
}

DimensionGroup::Iterator *DimensionGroupMaterialized::NewIterator(DimensionVector& dim)
{
	assert(no_dims == dim.Size());		// otherwise incompatible dimensions
	return new DGMaterializedIterator(no_obj, dim, t, nulls_possible);
}

/*
bool DimensionGroupMaterialized::IsThreadSafe()
{
	for(int d = 0; d < no_dims; d++)
		if(t[d] && t[d]->EndOfCurrentBlock(0) < t[d]->N())
			return false;
	return true;
}
*/

//// Iterator:

DimensionGroupMaterialized::DGMaterializedIterator::DGMaterializedIterator(_int64 _no_obj, DimensionVector& dims, IndexTable **_t, bool *nulls)
{
	no_obj = _no_obj;
	cur_pack_start = 0;
	no_dims = dims.Size();
	t = new IndexTable * [no_dims];
	one_packrow = new bool [no_dims];		// dimensions containing values from one packrow only
	nulls_found = new bool [no_dims];
	next_pack = new _int64 [no_dims];
	cur_pack = new int [no_dims];
	nulls_possible = nulls;
	inside_one_pack = false;				// to be determined later...
	for(int dim = 0; dim < no_dims; dim++) {
		one_packrow[dim] = false;
		nulls_found[dim] = false;
		cur_pack[dim] = -1;
		next_pack[dim] = 0;
		if(_t[dim] && dims[dim]) {
			t[dim] = _t[dim];
			one_packrow[dim] = (t[dim]->OrigSize() <= 0xFFFF) && (t[dim]->EndOfCurrentBlock(0) >= (_uint64)no_obj);
		} else
			t[dim] = NULL;
	}
	Rewind();
}

DimensionGroupMaterialized::DGMaterializedIterator::DGMaterializedIterator(const Iterator& sec) : DimensionGroup::Iterator(sec)
{
	DGMaterializedIterator* s = (DGMaterializedIterator*)(&sec);
	no_obj = s->no_obj;
	cur_pack_start = s->cur_pack_start;
	no_dims = s->no_dims;
	t = new IndexTable * [no_dims];
	one_packrow = new bool [no_dims];		// dimensions containing values from one packrow only
	nulls_found = new bool [no_dims];
	next_pack = new _int64 [no_dims];
	cur_pack = new int [no_dims];
	nulls_possible = s->nulls_possible;
	inside_one_pack = s->inside_one_pack;
	for(int dim = 0; dim < no_dims; dim++) {
		one_packrow[dim] = s->one_packrow[dim];
		nulls_found[dim] = s->nulls_found[dim];
		next_pack[dim] = s->next_pack[dim];
		cur_pack[dim] = s->cur_pack[dim];
		t[dim] = s->t[dim];
	}
	pack_size_left = s->pack_size_left;
	cur_pos = s->cur_pos;
}

DimensionGroupMaterialized::DGMaterializedIterator::~DGMaterializedIterator()
{
	delete [] t;
	delete [] one_packrow;
	delete [] nulls_found;
	delete [] next_pack;
	delete [] cur_pack;
}

void DimensionGroupMaterialized::DGMaterializedIterator::Rewind()
{ 
	cur_pos = 0; 
	cur_pack_start = 0;
	valid = true;
	for(int dim = 0; dim < no_dims; dim++)
		next_pack[dim] = 0;
	InitPackrow(); 
}

void DimensionGroupMaterialized::DGMaterializedIterator::InitPackrow()
{
	if(cur_pos >= no_obj) {
		valid = false;
		return;
	}
	_int64 cur_end_packrow = no_obj;
	for(int i = 0; i < no_dims; i++)
		if(t[i]) { 
			if(cur_pos >= next_pack[i]) {
				if(!one_packrow[i] || nulls_possible[i])
					FindPackEnd(i);
				else {
					next_pack[i] = no_obj;
					cur_pack[i] = int((t[i]->Get64(cur_pos) - 1) >> 16);
				}
			}
			if(next_pack[i] < cur_end_packrow)
				cur_end_packrow = next_pack[i];
	}
	pack_size_left = cur_end_packrow - cur_pos;	
	cur_pack_start = cur_pos;
	if(cur_pos == 0 && pack_size_left == no_obj)
		inside_one_pack = true;
}

bool DimensionGroupMaterialized::DGMaterializedIterator::NextInsidePack()				
{
	cur_pos++; 
	pack_size_left--; 
	if(pack_size_left == 0) {
		pack_size_left = cur_pos - cur_pack_start; 
		cur_pos = cur_pack_start; 
		return false; 
	}
	return true;
}


void DimensionGroupMaterialized::DGMaterializedIterator::FindPackEnd(int dim)
{
	_uint64 loc_iterator = next_pack[dim];
	_uint64 loc_limit = min((_uint64)no_obj, t[dim]->EndOfCurrentBlock(loc_iterator));
	int loc_pack = -1;
	nulls_found[dim] = false;
	if(!nulls_possible[dim]) {
		loc_pack = int((t[dim]->Get64(loc_iterator) - 1) >> 16);
		if(t[dim]->OrigSize() <= 0xFFFF) {
			loc_iterator = loc_limit;
		} else {
			++loc_iterator;
			while(loc_iterator < loc_limit && 		// find the first row from another pack (but the same block)
				(t[dim]->Get64InsideBlock(loc_iterator) - 1) >> 16 == loc_pack) {
					++loc_iterator;
			}
		}
	} else {
		while(loc_iterator < loc_limit && 	// find the first non-NULL row (NULL row is when Get64() = 0)
			t[dim]->Get64(loc_iterator) == 0) {
				nulls_found[dim] = true;
				++loc_iterator;
		}
		if(loc_iterator < loc_limit) {
			loc_pack = int((t[dim]->Get64(loc_iterator) - 1) >> 16);
			++loc_iterator;
			_uint64 ndx;
			while(loc_iterator < loc_limit && 		// find the first non-NULL row from another pack (but the same block)
				((ndx = t[dim]->Get64InsideBlock(loc_iterator)) == 0 ||
				((ndx - 1) >> 16) == loc_pack)) {
					if(ndx == 0)
						nulls_found[dim] = true;
					++loc_iterator;
			}
		}
	}
	cur_pack[dim] = loc_pack;
	next_pack[dim] = loc_iterator;
}

int DimensionGroupMaterialized::DGMaterializedIterator::GetNextPackrow(int dim, int ahead)
{
	if(ahead == 0)
		return GetCurPackrow(dim);
	if(ahead == 1) {
		if(t[dim] == NULL || next_pack[dim] >= no_obj || _uint64(next_pack[dim]) >= t[dim]->EndOfCurrentBlock(cur_pos))
			return -1;
		_uint64 next_pos = t[dim]->Get64(next_pack[dim]);
		if(next_pos == 0)
			return -1;
		return int((next_pos - 1) >> 16);
	}
	return t[dim]->GetNextPack(next_pack[dim],ahead-1);
	// orig code:
	//return -1;
}

bool DimensionGroupMaterialized::DGMaterializedIterator::BarrierAfterPackrow()			
{ 
	_int64 next_pack_start = cur_pos + pack_size_left;
	if(next_pack_start >= no_obj)
		return true;
	for(int i = 0; i < no_dims; i++)
		if(t[i] && (_uint64)next_pack_start >= t[i]->EndOfCurrentBlock(cur_pos))
			return true;
	return false; 
}
