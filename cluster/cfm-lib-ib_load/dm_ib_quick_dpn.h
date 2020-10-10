#ifndef __H_DM_IB_QUICK_DPN_H__
#define __H_DM_IB_QUICK_DPN_H__

#include <cr_class/cr_automsg.h>
#include <stdint.h>
#include <vector>
#include <string>

// Special value pack_file=-1 indicates "nulls only".
#define QUICKDPN_PF_NULLS_ONLY		(-1)
// Special value pack_file=-2 indicates "no objects".
#define QUICKDPN_PF_NO_OBJ		(-2)

class DM_IB_QuickDPN {
public:
	class QDPN {
	public:
		QDPN();
		~QDPN();

		void SetArgs(const uint8_t column_type);

		void PutNull();
		void PutValueI(const int64_t v);
		void PutValueD(const double v);
		void PutValueS(const char *v, const size_t vlen);

		size_t GetPrefixLen();

		void AppendToString(std::string &s) const;
		const char * ReadFromBuf(const char *buf);

		int32_t pack_file;		// save at buf[0 - 3]
		uint32_t pack_addr;		// save at buf[4 - 7]
		CR_Class_NS::union64_t min_v;	// save at buf[8 - 15]
		CR_Class_NS::union64_t max_v;	// save at buf[16 - 23]
		CR_Class_NS::union64_t sum_v;	// save at buf[24 - 31]
		uint32_t no_objs;		// save at buf[32 - 33]
		uint32_t no_nulls;		// save at buf[34 - 35]
	};

	DM_IB_QuickDPN();
	~DM_IB_QuickDPN();

	void SetArgs(const uint8_t column_type, const int64_t lineid_begin, const int64_t lineid_end);

	QDPN *LineToDPN(const int64_t lineid);

	int Save(int fd);

private:
	int64_t _lineid_begin;
	int64_t _lineid_end;
	int64_t _packid_begin;

	std::vector<QDPN> _dpn_array;
};

#endif /* __H_DM_IB_QUICK_DPN_H__ */
