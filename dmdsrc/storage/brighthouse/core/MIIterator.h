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

#ifndef MIITERATOR_H_INCLUDED
#define MIITERATOR_H_INCLUDED

#include "MultiIndex.h"
class PackOrderer;

/*! \brief Used for iteration on chosen dimensions in a MultiIndex.
 * Usage example:
 * MIIterator it(mind,...);
 * it.Rewind();
 * while(it.IsValid()) {
 *   ...
 *   ++it;
 * }
 *
 */
class MIIterator
{
public:
	/*! create an iterator for a MultiIndex and set to the \e mind begin
	 *
	 * \param mind the MultiIndex for which the iterator is created
	 * Example:
	 * 		DimensionVector dimensions(mind);
	 *		for(...)
	 *			dimensions[...we are interested in...] = true;
	 *		MIIterator mit(mind, dimensions);
	 *
	 */
	MIIterator(MultiIndex* mind);							// note: mind=NULL will create a local multiindex - one dim., one row
	MIIterator(MultiIndex* mind, int one_dimension, bool lock = true);		// -1 is an equivalent of all dimensions
	MIIterator(MultiIndex* mind, DimensionVector &dimensions);
	MIIterator(MultiIndex* mind, DimensionVector &dimensions, std::vector<PackOrderer>& po);	// Ordered iterator

	MIIterator(const MIIterator&, bool lock = true);
	MIIterator();							// empty constructor: only for special cases of MIDummyIterator
	~MIIterator();

	enum MIIteratorType {MII_NORMAL, MII_DUMMY, MII_LOOKUP};
	
	/*! Get the array of row numbers for each dimension in the MultiIndex for the current
	 * position of the iterator.
	 *
	 * \return Returns a pointer to a member array of row numbers with its length equal to the number of dimensions
	 * in the associated MultiIndex. Only the elements corresponding to dimensions used for MIIterator
	 * creation are set by the iterator, the rest is left as NULL_VALUE_64.
	 * Cannot be used at the end of MultiIndex (check IsValid() first).
	 */
	const _int64* operator*() const
	{ BHASSERT_WITH_NO_PERFORMANCE_IMPACT(valid); return cur_pos; }

	/*! Get the current value of iterator for the given dimension.
	 *
	 * \return Returns a row number in the associated MultiIndex.
	 * Only the elements corresponding to dimensions used for MIIterator
	 * creation are set by the iterator, the rest is left as NULL_VALUE_64.
	 * Cannot be used at the end of MultiIndex (check IsValid() first).
	 */
	const _int64 operator[](int n) const
	{ BHASSERT_WITH_NO_PERFORMANCE_IMPACT(valid && n >= 0 && n < no_dims); return cur_pos[n]; }

	MIIterator& operator=(const MIIterator& m) { MIIterator tmp(m); swap(tmp); return *this; }

	//! move to the next row position within an associated MultiIndex
	inline MIIterator& operator++()
	{
		assert(valid);
		next_pack_started = false;
		if(one_filter_dim > -1) {	// Optimized (one-dimensional filter) part:
			++(*one_filter_it);
			valid = one_filter_it->IsValid();
			--pack_size_left;
			if(valid) {
				cur_pos[one_filter_dim] = one_filter_it->GetCurPos(one_filter_dim);
				if(pack_size_left == 0) {
					cur_pack[one_filter_dim] = int(cur_pos[one_filter_dim] >> 16);
					InitNextPackrow();
				}
			}
		} else {	// General part:
			GeneralIncrement();
			if(pack_size_left != NULL_VALUE_64) {		// else the packrow is too large to be iterated through
				--pack_size_left;
				if(pack_size_left == 0)
					InitNextPackrow();
			}
		}
		return *this;
	}

	//! Skip a number of rows (equivalent of a number of ++it)
	void Skip(_int64 offset);

	//! set the iterator to the begin of the associated MultiIndex
	void Rewind();
	/*! True if the next call to operator* will return a valid result. False e.g. if end has been reached.
	 */
	bool IsValid() const					{ return valid; }

	MIIteratorType	Type() const			{ return mii_type; }

	/*! Get the number of rows omitted in unused dimensions on each step of iterator. Used e.g. as a factor in sum(...), count(...).
	 *
	 * \return Returns either a number of omitted rows,
	 * or NULL_VALUE_64 if the number is too big (> 2^63).
	 * \pre The result is correct for an initial state of multiindex (no further changes apply).
	 */
	_int64 Factor() const					{ return omitted_factor; 	}
	/*! Get the number of rows the iterator will iterate through.
	 *
	 * \return Returns either a number of rows to iterate,
	 * or NULL_VALUE_64 if the number is too big (> 2^63).
	 * \pre The result is correct for an initial state of multiindex (no further changes apply).
	 */
	virtual _int64 NoTuples() const					{ return no_obj; }
	//! Total number of tuples in the source multiindex 
	_int64 NoOrigTuples() const				{ return mind->NoTuples(); }
	//! How many rows (including the current one) are ahead of the current position in the current packrow
	virtual _int64 GetPackSizeLeft() const			{ return pack_size_left; }

	/*! True, if there were any null object numbers (NOT null values!) possibly found in the current packrow for a given dimension
	 * \warning True may mean that the null value is possible, but does not guarantee it!
	*/
	virtual bool NullsPossibleInPack(int dim) const		{ return it[it_for_dim[dim]]->NullsExist(dim); }

	virtual bool NullsPossibleInPack() const
	{
		for(int i = 0; i < no_dims; i++) {
			if(dimensions.Get(i) && it[it_for_dim[i]]->NullsExist(i)) return true;
		}
		return false;
	}

	/*! True, if the current packrow contain exactly the whole pack for a dimension (no repetitions, no null objects)
	*/
	virtual bool WholePack(int dim) const;

	/*!  Go to the beginning of the next packrow
	 * If the end is reached, IsValid() becomes false.
	 */
	virtual void NextPackrow();

	/*! Get pack numbers for each dimension in the associated MultiIndex for the current position.
	 * \return Returns a pointer to a member array of pack numbers with its length equal to the number of dimensions
	 * in the associated MultiIndex. Only the elements corresponding to dimensions used for MIIterator
	 * creation are set by the iterator.
	 */
	const int* GetCurPackrow() const		{ return cur_pack; }
	const int GetCurPackrow(int dim) const	{ return cur_pack[dim]; }
	virtual int GetNextPackrow(int dim, int ahead = 1) const;
	virtual uint GetRangeInPack(int dim,int p,void **pf) const;
	const int GetCurInpack(int dim) const
	{ BHASSERT_WITH_NO_PERFORMANCE_IMPACT(valid && dim >= 0 && dim < no_dims); return int(cur_pos[dim] & 0x000000000000FFFFLL); }

	//! True, if the current row is the first one in the packrow
	bool PackrowStarted()					{ return next_pack_started; }
	void swap(MIIterator&);

	virtual bool DimUsed(int dim) const		{ return dimensions.Get(dim); }	// is the dimension used in iterator?
	DimensionVector DimsUsed()				{ return dimensions; }

	int NoDimensions() const				{ return no_dims; }
	_int64 OrigSize(int dim) const			{ return mind->OrigSize(dim); }		// upper limit of a number of rows
	bool IsThreadSafe();
	bool BarrierAfterPackrow();				// true, if we must synchronize threads before NextPackrow() (including multiindex end)
  bool SetPackMttLoad(bool v=true) {ismttpackload=v;}
  bool IsPackMttLoad() const {return ismttpackload;}
protected:		////////////////////////////////////////////////////////////////////////////////

	void GeneralIncrement();		// a general-case part of operator++
	bool ismttpackload;
	vector<DimensionGroup::Iterator*>	it;			// iterators created for the groups
	vector<DimensionGroup*>				dg;			// external pointer: dimension groups to be iterated
	int*								it_for_dim;	// iterator no. for given dimensions
	///////////////////////////////////////////////////////////////////////////////
	MultiIndex*		mind;			// a multiindex the iterator is created for
	bool 			mind_created_locally;	// true, if the multiindex should be deleted in descructor
	int				no_dims;		// number of dimensions in the associated multiindex
	DimensionVector	dimensions;

	_int64*			cur_pos;		// a buffer storing the current position
	int*			cur_pack;		// a buffer storing the current pack number
	bool			valid;			// false if we are already out of multiindex

	_int64			omitted_factor;	// how many tuples in omitted dimensions are skipped at each iterator step
									// NULL_VALUE_64 means more than 2^63
	_int64			no_obj;			// how many steps (tuples) will the iterator perform
									// NULL_VALUE_64 means more than 2^63
	_int64			pack_size_left;	// how many steps (tuples) will the iterator
									// perform inside the current packrow, incl. the current one
	bool				next_pack_started;	// true if the current row is the first one inside the packrow
	MIIteratorType		mii_type;
	vector<PackOrderer>& po;	// for ordered iterator

	/////////// Optimized part ////////////////////////

	int one_filter_dim;		// if there is only one filter to be iterated, this is its number, otherwise -1
	DimensionGroup::Iterator* one_filter_it;	// external pointer: the iterator connected to the one dim, if any

private:
	void Init(bool lock = true);
	inline void InitNextPackrow() {		// find boundaries of the current packrow,
								// assumption: pack_size_left is properly updated only if the method is used at the first row of a packrow.
		if(!valid)
			return;
		pack_size_left = 1;
		for(int i = 0; i < it.size(); i++) {
			pack_size_left = SafeMultiplication(pack_size_left, it[i]->GetPackSizeLeft());
		}
		next_pack_started = true;
	}
};

/*! \brief Used for storing position of another iterator and for combining positions from several iterators.
 *
 */
class MIDummyIterator : public MIIterator
{
public:
	MIDummyIterator(int dims);						
	MIDummyIterator(MultiIndex* mind);
	MIDummyIterator(const MIIterator& sec) : MIIterator(sec) 	{ valid = true; }

	void Combine(const MIIterator& sec);	// copy position from the second iterator (only the positions used)
	void Set(int dim, _int64 val);			// set a position manually
	void SetPack(int dim, int p);			// set a position manually
	int GetNextPackrow(int dim, int ahead = 1) const { return MIIterator::GetCurPackrow(dim); }

	// These functions cannot be easily implemented for DummyIterator, so they return safe defaults
	bool NullsPossibleInPack(int dim) const	{ return true; }
	bool NullsPossibleInPack() const		{ return true; }
	bool WholePack(int dim) const			{ return false; }

	// These functions are illegal for MIDummyIterator
	MIIterator& operator++()				{ assert(0); }
	_int64 Factor() const					{ assert(0); return omitted_factor; }
	_int64 NoTuples() const					{ assert(0); return no_obj; }
	_int64 GetPackSizeLeft() const			{ assert(0); return pack_size_left; }
	void NextPackrow()						{ assert(0); }
	bool DimUsed(int dim) const				{ assert(0); return true; }
};

/*! \brief Used for storing position in lookup dictionary.
*
*/

class MILookupIterator : public MIDummyIterator
{
public:
	MILookupIterator() : MIDummyIterator(1)			{ mii_type = MII_LOOKUP; }						
	MILookupIterator(const MILookupIterator& sec) : MIDummyIterator(sec) {}

	void Set(_int64 val)			{ MIDummyIterator::Set(0, val); }
	void Invalidate()				{ valid = false; }
};

class MIInpackIterator : public MIIterator
{
public:
	MIInpackIterator(const MIIterator& sec) : MIIterator(sec) {}
	MIInpackIterator() : MIIterator() {}
	void swap(MIInpackIterator& i);
	MIInpackIterator& operator=(const MIInpackIterator& m) { MIInpackIterator tmp(m); swap(tmp); return *this; }
	MIInpackIterator& operator=(const MIIterator& m) { MIInpackIterator tmp(m); swap(tmp); return *this; }


	inline MIIterator& operator++()
	{
		assert(valid);
		next_pack_started = false;
		if(one_filter_dim > -1) {
			valid = one_filter_it->NextInsidePack();
			valid = valid && one_filter_it->IsValid();
			if(valid)
				cur_pos[one_filter_dim] = one_filter_it->GetCurPos(one_filter_dim);
		} else {
			// General part:
			GeneralIncrement();
		}
		--pack_size_left;
		if(pack_size_left == 0)
			valid = false;
		return *this;
	}

	void GeneralIncrement();
};

#endif /*MIITERATOR_H_INCLUDED*/
