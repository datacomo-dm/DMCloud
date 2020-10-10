#include "RSI_Framework.h"
#include "RSI_Histogram.h"
#include "RSI_CMap.h"

using namespace std;
using namespace ib_rsi;

const char* RSIndexName[] = { "TEST", "HIST", "JPP", "CMAP" };

//////////////////////////////////////////////////////////////

RSIndexID::RSIndexID(std::string fname){

}

inline int RSIndexID::Load(FILE* f)
{
	int typeIn = 0;
	if(1 != fscanf(f, "%18d ", &typeIn)) return 1;
	type = static_cast<RSIndexType>( typeIn ); /* *CAUTION* */
	if(!IsType1() && !IsType2()) return 1;
	if(2 != fscanf(f, "%18d %18d ", &tab, &col)) return 1;
	if(IsType2() &&
	  (2 != fscanf(f, "%18d %18d ", &tab2, &col2))) return 1;
	if(!IsCorrect()) return 1;
	return 0;
}

inline int RSIndexID::Save(FILE* f)
{
	if(0 > fprintf(f, "%d", type)) return 1;
	if(0 > fprintf(f, "\t%d\t%d", tab, col)) return 1;
	if(IsType2() &&
	  (0 > fprintf(f, "\t%d\t%d", tab2, col2))) return 1;
	if(0 > fprintf(f, "\n")) return 1;
	return 0;
}


bool RSIndexID::operator==(const RSIndexID& id) const
{
	return (col==id.col)&&(tab==id.tab)&&(type==id.type)&& (IsType1() || ((tab2==id.tab2)&&(col2==id.col2)));
}


bool RSIndexID::operator<(const RSIndexID& id) const
{
	if(type < id.type) return true; else
	if(type > id.type) return false;
	if(tab < id.tab) return true; else
	if(tab > id.tab) return false;
	if(col < id.col) return true; else
	if(col > id.col) return false;
	if(IsType1()) return false;
	assert(IsType2());
	if(tab2 < id.tab2) return true; else
	if(tab2 > id.tab2) return false;
	if(col2 < id.col2) return true;
	return false;
}

inline bool RSIndexID::Contain(int t, int c)
{
	if(c < 0) return (tab==t) || (IsType2() && (tab2==t));
	return ((tab==t)&&(col==c)) || (IsType2() && (tab2==t)&&(col2==c));
}

std::ostream& operator<<(std::ostream& outstr, const RSIndexID& rsi)
{
	switch(rsi.type) {
		case RSI_CMAP 	:
		case RSI_HIST 	:	outstr << rsi.type << "\t" << rsi.tab << "\t" << rsi.col ;
							break;
		case RSI_JPP	: 	outstr << rsi.type << "\t" << rsi.tab << "\t" << rsi.col << "\t" << rsi.tab2 << "\t" << rsi.col2 ;
							break;
		default			: 	break;
	}
	return outstr;
}

int ib_rsi::NoObj2NoPacks(_int64 no_obj){
	return (int) ((no_obj + 0xffff) >> 16);
}


int ib_rsi::CalculateBinSum(unsigned int n){
    //      NOTE: there is a potentially better method (to be tested and extended to 32 and 64 bits)
    //      The problem is known as "population function".
    //
    //      x = input (8 bit);
    //      x = x&55h + (x>>1)&55h;     x = x&33h + (x>>2)&33h;     x = x&0fh + (x>>4)&0fh
    //      result = x
    //

    int s=0;
    s+=bin_sums[n%256];
    n=n>>8;
    s+=bin_sums[n%256];
    n=n>>8;
    s+=bin_sums[n%256];
    n=n>>8;
    s+=bin_sums[n%256];
    return s;
}

