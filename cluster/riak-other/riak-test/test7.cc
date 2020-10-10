#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <cstddef>
#include <list>
#include <cr_class/cr_class.h>
#include <cr_class/cr_automsg.h>
#include <cr_class/cr_timer.h>
#include <cr_class/cr_memguard.h>

#ifndef MAX
#define MAX(a, b)	(((a)>(b))?(a):(b))
#endif /* MAX */

CR_AutoMsg **msg_arr = NULL;

int
max(int a, int b, void *_p)
{
  return MAX(a, b);
}

class test_class {
public:
  int max3(int a, int b, int c, void *_p) const
  {
    return MAX(a, MAX(b, c));
  }
};

void
msgarr_set(int num)
{
  if (msg_arr[num])
    return;

  CR_AutoMsg *msg_p;

  switch (num % 10) {
  case 0:
    msg_p = new CR_AutoMsg(num);
    break;
  case 1:
    msg_p = new CR_AutoMsg((double)num);
    break;
  case 2:
    msg_p = new CR_AutoMsg(CR_Class_NS::i642str(num).c_str());
    break;
  case 4:
    msg_p = new CR_AutoMsg(CR_Class_NS::i642str(num));
    break;
  case 5:
    msg_p = new CR_AutoMsg(std::bind(max, num, 0, std::placeholders::_1));
    break;
  default:
    msg_p = new CR_AutoMsg();
    break;
  }

  msg_arr[num] = msg_p;
}

void
msgarr_clr(int num)
{
  CR_AutoMsg *msg_p = msg_arr[num];

  if (!msg_p)
    return;

  switch (num % 10) {
  case 0:
    if (msg_p->as_int() != num) {
      THROWF("msg_p->as_int() == %d != %d\n", (int)msg_p->as_int(), num);
    }
    break;
  case 1:
    if (msg_p->as_double() != (double)num) {
      THROWF("msg_p->as_double() == %f != %f\n", msg_p->as_double(), (double)num);
    }
    break;
  case 2:
    if (msg_p->as_string() != CR_Class_NS::i642str(num)) {
      THROWF("msg_p->as_string() == %s != %s\n", msg_p->as_string().c_str(), CR_Class_NS::i642str(num).c_str());
    }
    break;
  case 4:
    if (msg_p->as_string() != CR_Class_NS::i642str(num)) {
      THROWF("msg_p->as_string() == %s != %s\n", msg_p->as_string().c_str(), CR_Class_NS::i642str(num).c_str());
    }
    break;
  case 5:
    if (msg_p->as_function()(NULL) != num) {
      THROWF("msg_p->as_function()(NULL) == %d != %d\n", (int)msg_p->as_function()(NULL), num);
    }
    break;
  default:
    break;
  }

  delete msg_p;

  msg_arr[num] = NULL;
}

int
main(int argc, char *argv[])
{
  size_t msg_arr_size = 0;
  size_t loop_count = 0;

  if (argc > 1)
    msg_arr_size = atoll(argv[1]);
  if (msg_arr_size < 1)
    msg_arr_size = 1;

  if (argc > 2)
    loop_count = atoll(argv[2]);
  if (loop_count < 1)
    loop_count = 1;

  msg_arr = new CR_AutoMsg*[msg_arr_size];

  for (size_t i=0; i<msg_arr_size; i++) {
    msg_arr[i] = NULL;
  }

  CR_MemGuard<char> buf(new char[100]);

  CR_MemGuard<char> buf1 = buf;

  CR_MemGuard<char> buf2;

  buf2 = CR_MemGuard<char>(new char[100]);

  buf[0] = '\0';

  CR_MemGuard<char> buf3(100);

  CR_MemGuard<char> buf4(100, '\0');

  CR_MemGuard<char> buf5 = buf4;

  buf1[99] = '\0';

//  buf2[100] = '\0';

//  buf5[100] = '\0';

  test_class test_c;
  static char _buf[] = "abc01234567890123456789abc01234567890123456789OK";
  CR_AutoMsg auto_msg(CR_AutoMsg::mb_desc_t(_buf, sizeof(_buf)));
  size_t _size_tmp;

  printf("%s\n", (char *)auto_msg.as_memblock(_size_tmp));

  auto_msg = 123;
  printf("%d,\t%s\n", auto_msg.as_int(), auto_msg.as_string().c_str());

  auto_msg = "abc123";
  printf("%s\n", auto_msg.as_c_str());

  auto_msg = std::string("abc456");
  printf("%s\n", auto_msg.as_c_str());

  auto_msg = "xyz01234567890123456789zyx01234567890123456789OK";
  printf("%s\n", auto_msg.as_c_str());

  printf("%d\n", (int)auto_msg.as_function()(NULL));

  auto_msg = std::bind(&test_class::max3, test_c, 33, 55, 44, std::placeholders::_1);
  printf("%d\n", (int)auto_msg.as_function()(&test_c));

  CR_AutoMsg auto_msg_test(auto_msg);
  printf("%d\n", (int)auto_msg_test.as_function()(&auto_msg_test));

  auto_msg_test = std::bind(max, 77, 88, std::placeholders::_1);
  printf("%d\n", (int)auto_msg_test.as_function()(&auto_msg));

  auto_msg = 123.0;
  printf("%d,\t%s\n", auto_msg.as_int(), auto_msg.as_string().c_str());

  auto_msg = "def4567";
  printf("%s\n", auto_msg.as_c_str());

  auto_msg = auto_msg.as_c_str();
  printf("%s\n", auto_msg.as_c_str());

  std::function<intmax_t(void*)> f1;
  std::string s1;

  f1 = std::bind(max, 77, 88, std::placeholders::_1);
  auto_msg = f1;

  DPRINTF("F:%d, S:%d, M:%d\n", (int)sizeof(f1), (int)sizeof(s1), (int)sizeof(auto_msg));

  CR_Timer msg_set_timer("msg_set_timer", NULL);
  CR_Timer msg_clr_timer("msg_clr_timer", NULL);

  for (size_t i=0; i<loop_count; i++) {
    int rand_val = rand() % msg_arr_size;
    if (msg_arr[rand_val]) {
      msg_set_timer.Start();
      msgarr_clr(rand_val);
      msg_set_timer.Stop();
    } else {
      msg_clr_timer.Start();
      msgarr_set(rand_val);
      msg_clr_timer.Stop();
    }
  }

  for (size_t i=0; i<msg_arr_size; i++) {
    msgarr_clr(i);
  }

  delete []msg_arr;

  return 0;
}
