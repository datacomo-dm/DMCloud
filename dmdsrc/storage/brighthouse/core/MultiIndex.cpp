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

#include "MultiIndex.h"
#include "MIIterator.h"
#include "Joiner.h"
#include "GroupDistinctTable.h"
#include "system/ConnectionInfo.h"
#include "tools.h"

using namespace std;

#define MATERIAL_TUPLES_LIMIT   150000000000LL			// = ~1 TB of cache needed for one dimension
#define MATERIAL_TUPLES_WARNING   2500000000LL			// = 10-20 GB of cache for one dimension

MultiIndex::MultiIndex()
	:	m_conn(ConnectionInfoOnTLS.IsValid()?&ConnectionInfoOnTLS.Get():NULL)
{
	// update Clear() on any change
	no_dimensions = 0;
	no_tuples = 0;
	no_tuples_too_big = false;
	dim_size = NULL;
	group_for_dim = NULL;
	group_num_for_dim = NULL;
	iterator_lock = 0;

	vector<bool> can_be_distinct;	// true if the dimension contain only one copy of original rows, false if we cannot guarantee this
	vector<bool> used_in_output;	// true if given dimension is used for generation of output columns
	vector<DimensionGroup*> dim_groups;		// all active dimension groups
}

MultiIndex::MultiIndex(const MultiIndex &s)
	:	m_conn(s.m_conn)
{
	no_dimensions = s.no_dimensions;
	no_tuples = s.no_tuples;
	no_tuples_too_big = s.no_tuples_too_big;
	if(no_dimensions>0)
	{
		dim_size = new _int64 [no_dimensions];
		group_for_dim = new DimensionGroup* [no_dimensions];
		group_num_for_dim = new int [no_dimensions];
		used_in_output = s.used_in_output;
		can_be_distinct = s.can_be_distinct;
		for(int i = 0; i < s.dim_groups.size(); i++)
			dim_groups.push_back(s.dim_groups[i]->Clone(false));

		for(int i = 0; i < no_dimensions; i++)
			dim_size[i] = s.dim_size[i];

		FillGroupForDim();
	} else {
		dim_size = NULL;
		group_for_dim = NULL;
		group_num_for_dim = NULL;
	}
	iterator_lock = 0;
}

void MultiIndex::FillGroupForDim()
{
	MEASURE_FET("MultiIndex::FillGroupForDim(...)");
	int move_groups = 0;
	for(int i = 0; i < dim_groups.size(); i++) {		// pack all holes
		if(dim_groups[i] == NULL) {
			while(i + move_groups < dim_groups.size() && dim_groups[i + move_groups] == NULL)
				move_groups++;
			if(i + move_groups < dim_groups.size()) {
				dim_groups[i] = dim_groups[i + move_groups];
				dim_groups[i + move_groups] = NULL;
			} else
				break;
		}
	}
	for(int i = 0; i < move_groups; i++)
		dim_groups.pop_back();								// clear nulls from the end

	for(int d = 0; d < no_dimensions; d++) {
		group_for_dim[d] = NULL;
		group_num_for_dim[d] = -1;
	}

	for(int i = 0; i < dim_groups.size(); i++) {
		for(int d = 0; d < no_dimensions; d++)
			if(dim_groups[i]->DimUsed(d)) {
				group_for_dim[d] = dim_groups[i];
				group_num_for_dim[d] = i;
			}
	}
}

void MultiIndex::Copy(MultiIndex &s, bool shallow)			// copy the object s
{
	MEASURE_FET("MultiIndex::Copy(...)");
	for(int i = 0; i < dim_groups.size(); i++) {
		delete dim_groups[i];
		dim_groups[i] = NULL;
	}
	dim_groups.clear();
	delete [] dim_size;
	delete [] group_for_dim;
	delete [] group_num_for_dim;
	/////////////////////////////////////////
	no_dimensions = s.no_dimensions;
	no_tuples = s.no_tuples;
	no_tuples_too_big = s.no_tuples_too_big;
	if(no_dimensions > 0) {
		group_for_dim = new DimensionGroup* [no_dimensions];
		group_num_for_dim = new int [no_dimensions];
		dim_size = new _int64 [no_dimensions];
		used_in_output = s.used_in_output;
		can_be_distinct = s.can_be_distinct;
		for(int i = 0; i < s.dim_groups.size(); i++)
			dim_groups.push_back(s.dim_groups[i]->Clone(shallow));

		for(int i = 0; i < no_dimensions; i++)
			dim_size[i] = s.dim_size[i];

		FillGroupForDim();
	} else {
		dim_size = NULL;
		group_for_dim = NULL;
		group_num_for_dim = NULL;
	}
	iterator_lock = 0;
}

MultiIndex::~MultiIndex()
{
	try {
		for(int i = 0; i < dim_groups.size(); i++) {
			delete dim_groups[i];
			dim_groups[i] = NULL;
		}
	} catch ( ... ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ! "exception from destructor" );
	}
	delete [] dim_size;
	delete [] group_for_dim;
	delete [] group_num_for_dim;
}

void MultiIndex::Clear()
{
	try {
		for(int i = 0; i < dim_groups.size(); i++) {
			delete dim_groups[i];
			dim_groups[i] = NULL;
		}
	} catch ( ... ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ! "exception from destructor" );
	}
	delete [] dim_size;
	delete [] group_for_dim;
	delete [] group_num_for_dim;
	dim_groups.clear();
	no_dimensions = 0;
	no_tuples = 0;
	no_tuples_too_big = false;
	dim_size = NULL;
	group_for_dim = NULL;
	group_num_for_dim = NULL;
	iterator_lock = 0;
	can_be_distinct.clear();
	used_in_output.clear();
}

void MultiIndex::Empty(int dim_to_make_empty)
{
	if(dim_to_make_empty != -1)
		group_for_dim[dim_to_make_empty]->Empty();
	else {
		for(int i = 0; i < dim_groups.size(); i++)
			dim_groups[i]->Empty();
	}
	for(int i = 0; i < no_dimensions; i++) {
		if(dim_to_make_empty == -1 || group_for_dim[dim_to_make_empty]->DimUsed(i)) {
			can_be_distinct[i] = true;
			used_in_output[i] = true;
		}
	}
	no_tuples = 0;
	no_tuples_too_big = false;
}

void MultiIndex::AddDimension()
{
	no_dimensions++;
	_int64*			ns = new _int64 [no_dimensions];
	DimensionGroup** ng = new DimensionGroup* [no_dimensions];
	int*			ngn = new int [no_dimensions];
	for(int i = 0; i < no_dimensions - 1; i++) {
		ns[i] = dim_size[i];
		ng[i] = group_for_dim[i];
		ngn[i] = group_num_for_dim[i];
	}
	delete [] dim_size;
	delete [] group_for_dim;
	delete [] group_num_for_dim;
	dim_size = ns;
	group_for_dim = ng;
	group_num_for_dim = ngn;
	dim_size[no_dimensions - 1] = 0;
	group_for_dim[no_dimensions - 1] = NULL;
	group_num_for_dim[no_dimensions - 1] = -1;
	// Temporary code, for rare cases when we add a dimension after other joins (smk_33):
	for(int i = 0; i < dim_groups.size(); i++)
		dim_groups[i]->AddDimension();

	return;		// Note: other functions will use AddDimension() to enlarge tables
}

// adjtumples is true and pnfblocks no null,do the no_tuples adjust
void MultiIndex::AddDimension_cross(_uint64 size,std::map<int,int> *pnfblocks,bool adjtumples) 
{
	AddDimension();
	int new_dim = no_dimensions - 1;
	//DMA-678,query crash on RoughMultiIndex clear
	// pack number keep original value in filters:
	_uint64 old_size=size;
	used_in_output.push_back(true);
	if(adjtumples&&pnfblocks&&size>0) {
		int lastp=(size-1)/65536;
		for(std::map<int,int>::iterator it=pnfblocks->begin();it!=pnfblocks->end();it++) {
			if(it->first!=lastp) size-=(65536-it->second);
		}
	}
	if(no_dimensions > 1)
		MultiplyNoTuples(size);
	else
		no_tuples = size;
	DimensionGroupFilter *nf = NULL;
	std::map<int,int> nfblocks;
	if(size > 0) {
		dim_size[new_dim] = old_size;//size;
		nf = new DimensionGroupFilter(new_dim, old_size,pnfblocks!=NULL?pnfblocks:&nfblocks);
	} else {		// size == 0   => prepare a dummy dimension with empty filter
		dim_size[new_dim] = 1;
		nf = new DimensionGroupFilter(new_dim, dim_size[new_dim],pnfblocks!=NULL?pnfblocks:&nfblocks);
		nf->Empty();
	}
	dim_groups.push_back(nf);
	group_for_dim[new_dim] = nf;
	group_num_for_dim[new_dim] = int(dim_groups.size() - 1);
	can_be_distinct.push_back(true);		// may be modified below
	CheckIfVirtualCanBeDistinct();
}

void MultiIndex::AddDimension_cross(Filter& new_f)
{
	AddDimension();
	int new_dim = no_dimensions - 1;
	DimensionGroupFilter *nf = new DimensionGroupFilter(new_dim, &new_f);
	dim_groups.push_back(nf);
	group_for_dim[new_dim] = nf;
	group_num_for_dim[new_dim] = int(dim_groups.size() - 1);
	if(no_dimensions > 1)
		MultiplyNoTuples(new_f.NoOnes());
	else
		no_tuples = new_f.NoOnes();
	dim_size[new_dim] = new_f.NoObj();
	used_in_output.push_back(true);
	can_be_distinct.push_back(true);
	CheckIfVirtualCanBeDistinct();
}

void MultiIndex::MultiplyNoTuples( _uint64 factor )
{
	no_tuples = SafeMultiplication(no_tuples, factor);
	if(no_tuples == NULL_VALUE_64)
		no_tuples_too_big = true;
}

////////////////////////////////////////////////////////////////////////////

void MultiIndex::CheckIfVirtualCanBeDistinct()	// updates can_be_distinct table in case of virtual multiindex
{
	// check whether can_be_distinct can be true
	// it is possible only when there are only 1-object dimensions
	// and one multiobject (then the multiobject one can be distinct, the rest cannot)
	if(no_dimensions > 1)	{
		int non_one_found = 0;
		for(int i = 0; i < no_dimensions; i++) {
			if(dim_size[i] > 1)
				non_one_found++;
		}
		if(non_one_found == 1)
			for(int j = 0; j < no_dimensions; j++)
				if(dim_size[j] == 1)
					can_be_distinct[j] = false;
				else
					can_be_distinct[j] = true;
		if(non_one_found > 1)
			for(int j = 0; j < no_dimensions; j++)
				can_be_distinct[j] = false;
	}
}

void MultiIndex::LockForGetIndex(int dim) 
{
	group_for_dim[dim]->Lock(dim);
}

void MultiIndex::UnlockFromGetIndex(int dim) 
{
	group_for_dim[dim]->Unlock(dim);
}


_uint64 MultiIndex::DimSize(int dim)				// the size of one dimension: material_no_tuples for materialized, NoOnes for virtual
{
	return group_for_dim[dim]->NoTuples();
}


void MultiIndex::LockAllForUse()
{
	for(int dim = 0; dim < no_dimensions; dim++)
		LockForGetIndex(dim);
}

void MultiIndex::UnlockAllFromUse()
{
	for(int dim = 0; dim < no_dimensions; dim++)
		UnlockFromGetIndex(dim);
}

void MultiIndex::MakeCountOnly(_int64 mat_tuples, DimensionVector& dims_to_materialize)
{
	MEASURE_FET("MultiIndex::MakeCountOnly(...)");
	MarkInvolvedDimGroups(dims_to_materialize);
	for(int i = 0; i < NoDimensions(); i++) {
		if(dims_to_materialize[i] && group_for_dim[i] != NULL) {
			// delete this group
			int dim_group_to_delete = group_num_for_dim[i];
			for(int j = i; j < NoDimensions(); j++)
				if(group_num_for_dim[j] == dim_group_to_delete) {
					group_for_dim[j] = NULL;
					group_num_for_dim[j] = -1;
				}
			delete dim_groups[dim_group_to_delete];
			dim_groups[dim_group_to_delete] = NULL;		// these holes will be packed in FillGroupForDim()
		}
	}
	DimensionGroupMaterialized* count_only_group = new DimensionGroupMaterialized(dims_to_materialize);
	count_only_group->SetNoObj(mat_tuples);
	dim_groups.push_back(count_only_group);
	FillGroupForDim();
	UpdateNoTuples();
}

void MultiIndex::UpdateNoTuples()
{
	// assumptions:
	// - no_material_tuples is correct, even if all t[...] are NULL (forgotten).
	//   However, if all f[...] are not NULL, then the index is set to IT_VIRTUAL and no_material_tuples = 0
	// - IT_MIXED or IT_VIRTUAL may be in fact IT_ORDERED (must be set properly on output)
	// - if no_material_tuples > 0, then new index_type is not IT_VIRTUAL
	no_tuples_too_big = false;
	if(dim_groups.size() == 0)
		no_tuples = 0;
	else {
		no_tuples = 1;
		for(int i = 0; i < dim_groups.size(); i++) {
			dim_groups[i]->UpdateNoTuples();
			MultiplyNoTuples(dim_groups[i]->NoTuples());
		}
	}
}

_int64 MultiIndex::NoTuples(DimensionVector &dimensions, bool fail_on_overflow)		// for a given subset of dimensions
{
	vector<int> dg = ListInvolvedDimGroups(dimensions);
	if(dg.size() == 0)
		return 0;
	_int64 res = 1;
	for(int i = 0; i < dg.size(); i++) {
		dim_groups[dg[i]]->UpdateNoTuples();
		res = SafeMultiplication(res, dim_groups[dg[i]]->NoTuples());
	}
	if(res == NULL_VALUE_64 && fail_on_overflow)
		throw OutOfMemoryRCException("Too many tuples.  (1428)");
	return res;
}

int MultiIndex::MaxNoPacks(int dim)	// maximal (upper approx.) number of different nonempty data packs for the given dimension
{
	int max_packs = 0;
	Filter *f = group_for_dim[dim]->GetFilter(dim);
	if(f) {
		for(int p = 0; p < f->NoBlocks(); p++)
			if(!f->IsEmpty(p))
				max_packs++;
	} else {
		max_packs = int((dim_size[dim]) >> 16) + 1;
		if(group_for_dim[dim]->NoTuples() < max_packs)
			max_packs = (int)group_for_dim[dim]->NoTuples();
	}
	return max_packs;
}

//////////////////////////////// Logical operations //////////////////////////////

void MultiIndex::MIFilterAnd(MIIterator &mit, Filter &fd)	// limit the MultiIndex by excluding all tuples which are not present in fd, in order given by mit
{
	MEASURE_FET("MultiIndex::MIFilterAnd(...)");
	LockAllForUse();
	if(no_dimensions == 1 && group_for_dim[0]->GetFilter(0)) {
		Filter *f = group_for_dim[0]->GetFilter(0);
		FilterOnesIterator fit(f);
		_int64 cur_pos = 0;
		while(fit.IsValid()) {
			if(!fd.Get(cur_pos))
				fit.ResetDelayed();
			++fit;
			cur_pos++;
		}
		f->Commit();
		UpdateNoTuples();
		UnlockAllFromUse();
		return;
	}
	
	DimensionVector dim_used(mit.DimsUsed());
	MarkInvolvedDimGroups(dim_used);
	_int64 new_no_tuples = fd.NoOnes();
	JoinTips tips(*this);
	MINewContents new_mind(this, tips);
	new_mind.SetDimensions(dim_used);
	new_mind.Init(new_no_tuples);
	mit.Rewind();
	_int64 f_pos = 0;
	while(mit.IsValid()) {
		if(fd.Get(f_pos)) {
			for(int d = 0; d < no_dimensions; d++)
				if(dim_used[d])
					new_mind.SetNewTableValue(d, mit[d]);
			new_mind.CommitNewTableValues();
		}
		++mit;
		f_pos++;
	}
	new_mind.Commit(new_no_tuples);
	UpdateNoTuples();
	UnlockAllFromUse();
}

bool MultiIndex::MarkInvolvedDimGroups(DimensionVector &v)	// if any dimension is marked, then mark the rest of its group
{
	bool added = false;
	for(int i = 0; i < no_dimensions; i++) {
		if(!v[i]) {
			for(int j = 0; j < no_dimensions; j++) {
				if(v[j] && group_num_for_dim[i] == group_num_for_dim[j]) {
					v[i] = true;
					added = true;
					break;
				}
			}	
		}
	}
	return added;
}

vector<int> MultiIndex::ListInvolvedDimGroups(DimensionVector &v)	// List all internal numbers of groups touched by the set of dimensions
{
	vector<int> res;
	int cur_group_number;
	for(int i = 0; i < no_dimensions; i++) {
		if(v[i]) {
			cur_group_number = group_num_for_dim[i];
			bool added = false;
			for(int j = 0; j < res.size(); j++)
				if(res[j] == cur_group_number) {
					added = true;
					break;
				}
			if(!added)
				res.push_back(cur_group_number);
		}
	}
	return res;
}

std::string MultiIndex::Display()
{
	vector<int> ind_tab_no;
	int it_count = 0;
	for(int i = 0; i < dim_groups.size(); i++) {
		if(dim_groups[i]->Type() == DimensionGroup::DG_INDEX_TABLE) {
			it_count++;
			ind_tab_no.push_back(it_count);								// calculate a number of a materialized dim. group
		} else
			ind_tab_no.push_back(0);
	}

	///////////////////////////
	std::string s;
	s = "[";
	for(int i = 0; i < no_dimensions; i++) {
		if(!group_for_dim[i]->DimEnabled(i))
			s += "-";
		else if(group_for_dim[i]->Type() == DimensionGroup::DG_FILTER)
			s += "f";
		else if(group_for_dim[i]->Type() == DimensionGroup::DG_INDEX_TABLE)
			s += (ind_tab_no[group_num_for_dim[i]] > 9 ? 'T' : '0' + ind_tab_no[group_num_for_dim[i]]);
		else if(group_for_dim[i]->Type() == DimensionGroup::DG_VIRTUAL)
			s += (GetFilter(i) ? 'F' : 'v');
		else
			s += "?";
	}
	s += "]";
	return s;
}

