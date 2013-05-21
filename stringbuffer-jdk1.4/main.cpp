// This file is used to mimic the StringBuffer bug in JDK1.4
// Author: Jie Yu (jieyu@umich.edu)

#include "stringbuffer.hpp"

StringBuffer *buffer = new StringBuffer("abc");

void *thread_main(void *args) {
  while (1) {
    buffer->erase(0, 3);
    buffer->append("abc");
  }
}

int main(int argc, char *argv[]) {
  pthread_t thd;
  int rc;

  rc = pthread_create(&thd, NULL, thread_main, NULL);

  while (1) {
    StringBuffer *sb = new StringBuffer();
    sb->append(buffer);
  }

  return 0;
}

