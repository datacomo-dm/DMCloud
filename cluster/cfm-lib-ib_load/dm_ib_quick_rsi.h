#ifndef __H_DM_IB_QUICK_RSI_H__
#define __H_DM_IB_QUICK_RSI_H__

#include <stdint.h>

class DM_IB_QuickRSI {
public:
	DM_IB_QuickRSI();
	~DM_IB_QuickRSI();

	void SetArgs(const uint8_t column_type, const size_t column_scale,
		const int64_t lineid_begin, const int64_t lineid_end);

	int PutValueN(const int64_t lineid, const int64_t v, const int64_t minv, const int64_t maxv);
	int PutValueS(const int64_t lineid, const char *v, const size_t vlen, const size_t prefix_len);

	int Save(int fd);

private:
	uint8_t _column_type;
	void *_rsi_handle;
	int64_t _lineid_begin;
	int64_t _lineid_end;
	int64_t _packid_begin;
};

#endif /* __H_DM_IB_QUICK_RSI_H__ */
