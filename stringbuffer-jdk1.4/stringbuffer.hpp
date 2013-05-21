// This file is used to mimic the StringBuffer bug in JDK1.4
// Author: Jie Yu (jieyu@umich.edu)

#ifndef STRINGBUFFER_HPP_
#define STRINGBUFFER_HPP_

#include <pthread.h>

#define INTEGER_MAX_VALUE 0x7fffffff

class StringBuffer {
 public:
  StringBuffer();
  explicit StringBuffer(int length);
  explicit StringBuffer(char *str);
  ~StringBuffer();

  int length();
  void getChars(int srcBegin, int srcEnd, char *dst, int dstBegin);
  StringBuffer *append(StringBuffer *sb);
  StringBuffer *append(char *str);
  StringBuffer *erase(int start, int end);
  void print();

 private:
  char *value;
  int value_length;
  int count;
  pthread_mutex_t mutex_lock;

  static StringBuffer *null_buffer;

  void expandCapacity(int minimumCapacity);
};

#endif
