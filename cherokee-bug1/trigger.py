""" To trigger the cherokee bug2 in cherokee-0.9.2.
"""

import os
import sys
import subprocess
import multiprocessing

class Client(multiprocessing.Process):
    def __init__(self, num_calls):
        multiprocessing.Process.__init__(self)
        self.num_calls = num_calls
    def run(self):
        for i in range(self.num_calls):
            fnull = open(os.devnull, 'w')
            subprocess.call('wget -O /dev/null --header=\'If-Modified-Since: Sat Oct 1994 19:43:31 GMT\' http://127.0.0.1/index.html', stdout=fnull, stderr=fnull, shell=True)
            fnull.close()

def main(argv):
    num_calls = int(argv[1])
    client1 = Client(num_calls)
    client2 = Client(num_calls)
    client1.start()
    client2.start()
    client1.join()
    client2.join()

if __name__ == '__main__':
    main(sys.argv)

