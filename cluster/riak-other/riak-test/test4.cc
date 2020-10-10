#include <string.h>
#include <stdlib.h>
#include <cr_class/cr_class.h>
#include <cr_class/cr_compress.h>
#include <cr_class/cr_memguard.h>
#include <iostream>
#include <errno.h>

int main(int argc, char *argv[])
{
	int ret;

	if (argc < 5) {
		fprintf(stderr,
		  "Usage: %s <type_code | 'de'> <preset> src_filename dst_filename\n",
		  argv[0]);
		exit(1);
	}

	std::string src_filename, dst_filename;
	std::string src_data, dst_data;
	char type_code = argv[1][0];
	bool do_decompress = false;
	int preset = atoi(argv[2]);
	double enter_time, usage_time;

       	if (strcmp(argv[1], "de") == 0)
		do_decompress = true;

	src_filename = argv[3];
	dst_filename = argv[4];

	CR_Class_NS::load_string(src_filename.c_str(), src_data);

	enter_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);
	if (do_decompress) {
		ret = CR_Compress::decompress(src_data, dst_data);
	} else {
		ret = CR_Compress::compress(src_data, dst_data, type_code, preset);
	}
	usage_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - enter_time;

	printf("%s, time usage: %f seconds\n", CR_Class_NS::strerrno(ret), usage_time);

	if (!ret) {
		CR_Class_NS::save_string(dst_filename.c_str(), dst_data);
	}

	return ret;
}
