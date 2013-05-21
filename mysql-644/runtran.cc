#include <math.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <strings.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>

#include <string>
#include <ext/hash_map>
#include <vector>
#include <iostream>
#include <iomanip>
#include <fcntl.h>
#include <fstream>

#include <mysql/mysql.h>

#define MYSQL_SOCK_FILE "/tmp/mysql.sock"
#define MYSQL_PATH "/opt/bugs/mysql-4.1.1/mysql-4.1.1-alpha/bin"

// g++ -g3 -O2 -Wshadow -lpthread -Wall -L/home/mysql-4.1.1-alpha/mysql/lib/mysql -Wl,-R/home/mysql-4.1.1-alpha/mysql/lib/mysql -I/home/mysql-4.1.1-alpha/mysql/include  runtran.cc -lmysqlclient -lz -o runtran
using namespace std;

///function for telling the compiler that is should expect this to evaluate to false
#define unlikely(exp) __builtin_expect((exp),0)

#define ABORTIF(XXX) do { if (unlikely(XXX)){cout << " at line " << __LINE__ << endl; abort();} } while(0)
#define ABORT() do { cout << " at line " << __LINE__ << endl; abort(); } while(0)
#define EABORT() do { if (errno) {cout << strerror(errno) << " dying ... ";} ABORT(); } while(0)
#define MABORT() do { if (&dbase) {cout << mysql_error(&dbase) << " dying ... ";} ABORT(); } while(0)
#define MSGABORT(MGS) do { cout << MGS << " at line " << __LINE__ << endl; abort(); } while(0)

/**
  Determine if cond is false and if not throw an exception.

  @param COND condition to evaluate
  @param MSG the message to print along with an errno
*/
#define ERR(COND, MSG) if(unlikely(COND)){cout << MSG << endl; EABORT(); }

using __gnu_cxx::hash_map;
/** compare two strings */
struct eqstr { /** compare */ bool operator()(const char* s1, const char* s2) const { return strcmp(s1, s2) == 0; } }; 
enum stm_type_t {
     BEGIN,
     COMMIT,
     ROLLBACK,
     SELECT,
     TEMPTPL,
     WRITE //must be last
};

/** A query, with string and type */
struct aquery {
     ///Constructor
     aquery(enum stm_type_t type, const string& qstr) : 
          q(qstr), t(type) { }
     string q; ///< Query string
     enum stm_type_t t; ///< Type
};

/** calc the difference between two timevals */
static inline
void gettimediffs(struct timeval& res, const struct timeval &now, const struct timeval &last) {
     res.tv_usec = now.tv_usec - last.tv_usec;
     res.tv_sec = now.tv_sec - last.tv_sec;
     if (res.tv_usec < 0) {
          res.tv_sec-=1;
          res.tv_usec+=1000000;
     }
}

/** keeps the list of completed queries along with their completion time etc. */
struct result {
     const string host; ///< Host the query was executed on
     const struct timeval start, end; ///< Start and end times
     const struct aquery *thequery; ///< Pointer to the query

     /// Constructor
     result(const struct aquery *q, const char * h,
            const struct timeval &s, const struct timeval &e)
          : host(string(h)), start(s), end(e), thequery(q) {}

     /// Print the statistics to a file
     std::ostream& operator<<(std::ostream& o) const {
          struct timeval t;
          gettimediffs(t, end, start);
          return o
               << thequery->t << " "
               << host << " "
//             << start.tv_sec << "."
//             << start.tv_usec << " "
//             << end.tv_sec << "."
//             << end.tv_usec << " "
               << t.tv_sec << "."
               << setfill('0') << setw(6) <<  t.tv_usec << " \""
               << thequery->q << "\"";
     }
};

std::ostream& operator<<(std::ostream& o, const struct result& r) {
  return r.operator<<(o);
}

static vector< vector<aquery> > queries; ///< array of transactions which are arrays of queries
static unsigned int gtid = 0; ///< next transaction id available for execution
static pthread_mutex_t gtid_m = PTHREAD_MUTEX_INITIALIZER; ///< to protect gtid
static volatile bool rampupdone = 0;

/** Groups all results from a run together */
class resultset_t {
public:
     vector<const struct result*> results; ///< Vector of results
     unsigned int seed; ///< The random seed we started with
     const int clientid; ///< The clientid for the thread

     /** Constructor */
     resultset_t(int clentid) : clientid(clentid) {}

     /** push result to list if we are done with the rampup */
     void update(const struct result* res) {
          if (rampupdone)
               results.push_back(res);
     }

     /** print it */
     std::ostream& operator<<(std::ostream& o) const {
          for (unsigned int i = 0; i < results.size(); i++)
               o << clientid << " " << *results[i] << endl;
          return o;
     }
};

static char* tracefile = "outc.txt"; ///< default trace filename
static bool repeatlog = 0; ///< default stop when log runs out
static int sleeptimeg = -1; ///< Time to sleep between queries (-1 = dont sleep, 0 = tpcw thinktime, other = that)
static int allowwrite = 0; ///< default dont allow writes

/** A SQLgenerator, currently works by reading a tracefile */
class SQLGenerator {
private:
     vector<aquery>::const_iterator it; ///< current iterator of queries executing
     unsigned int tid; ///< Current transaction id for this thread
     MYSQL* dbase; ///< Database connectiom

     bool ntid; ///< new tid allocated since last statement
public:
     ///True if the last statement was the last of a transaction
     ///sequence, used to verify we issued a commit or rollback
     bool last_stm_was_new_tid() const { return ntid; }

     ///Get a new transaction id
     void newtid() {
          ABORTIF(pthread_mutex_lock(&gtid_m));
          tid = gtid++;
          ABORTIF(pthread_mutex_unlock(&gtid_m));
     }

     /** 
         Reinit the global transaction id counter
         
         \bug this function could be called several times, so that
         start of a trace might be run several times */
     static void reinit() {
          ABORTIF(pthread_mutex_lock(&gtid_m));
          gtid = 0;
          ABORTIF(pthread_mutex_unlock(&gtid_m));
     }

     ///Constructor, reads in tracefile
     SQLGenerator(MYSQL* adbase) : dbase(adbase), ntid(0) {
          ABORTIF(pthread_mutex_lock(&gtid_m));
          if (!queries.size()) {
               char buf[1024];
               FILE* f = fopen(tracefile, "r");
               if (!f) {
                    cout << "Can not open " << tracefile << endl;
                    EABORT();
               }
               while(fgets(buf,1024,f)) {
                    unsigned int nr;
                    int where;
                    char*p=rindex(buf, '\n');
                    if (p) *p='\0';
                    if (sscanf(buf,"%*s %*s B %u", &nr)) {
                         if (nr > queries.size()) queries.resize(nr);
                         queries[nr-1].push_back(aquery(BEGIN, string()));
                    } else if (sscanf(buf,"%*s %*s C %u", &nr)) {
                         if (nr > queries.size()) queries.resize(nr);
                         queries[nr-1].push_back(aquery(COMMIT, string()));
                    } else if (sscanf(buf,"%*s %*s R %u", &nr)) {
                         if (nr > queries.size()) queries.resize(nr);
                         queries[nr-1].push_back(aquery(ROLLBACK, string()));
                    } else if (sscanf(buf,"%*s %*s S %u %n", &nr, &where)) {
                         if (nr > queries.size()) queries.resize(nr);
                         queries[nr-1].push_back(aquery(SELECT, string(buf+where)));
                    } else if (sscanf(buf,"%*s %*s W %u %n", &nr, &where)) {
                         if (nr > queries.size()) queries.resize(nr);
                         if (strncmp(buf+where, "create temporary", strlen("create temporary")) == 0 ||
                             strncmp(buf+where, "drop table", strlen("drop table")) == 0) {
                              queries[nr-1].push_back(aquery(TEMPTPL, string(buf+where)));
                         } else {
                              queries[nr-1].push_back(aquery(WRITE, string(buf+where)));
                         }
                    } else {
                         cout << "Ignored unknown line " << buf;
                    }
               }
               fclose(f);
          }
          ABORTIF(pthread_mutex_unlock(&gtid_m));

          newtid();
          //force a new tid selection next time
          it = queries[tid].begin();
     }

     /**
        @return amount of time to sleep till next query (-1 if dont
        sleep, 0 if stop), t = query to execute next
      */
     const struct aquery* getnext(resultset_t* res, int *sleeptime) {
          if (it == queries[tid].end() || tid >= queries.size()) {
               newtid();
               ntid = 1;
               if (tid >= queries.size()) {
                    *sleeptime = 0;
                    return &(*it); //invalid
               }
               it = queries[tid].begin();
          }
          else {
               ntid = 0;
          }

          const struct aquery* q = &(*it);
          ++it;

          if (sleeptimeg == 0) {
               //simulate tpcw sleeptime
               double r = (static_cast<double>(rand())/(RAND_MAX+1.0));
               if (r < 4.54e-5)
                    *sleeptime = static_cast<int>((r+0.5)*1000);
               else
                    *sleeptime = static_cast<int>(((-7000.0*log(r))+0.5)*1000);
          }
          else
               *sleeptime = sleeptimeg;
          return q;
     }
     
};

std::ostream& operator<<(std::ostream& o, const resultset_t& r) { return r.operator<<(o); }

static char* host = "localhost"; ///< default host to connect to
static char* user = "root"; ///< default user
static char* pass = ""; ///< no password
static char* database = "test"; ///< default database
static const char * mysqlsock = MYSQL_SOCK_FILE;
static const char * mysqlpath = MYSQL_PATH;
static const char* sarpath = "/usr/local/bin/sar"; ///< path to sar
static const char* sshpath = "/usr/bin/ssh"; ///< path to ssh
static const char* saroptions[6] = { "-n", "DEV", "-n", "SOCK", "-rubcw", "1" }; ///< options to sar
static int NRTHR = 3; ///< default number of concurrent threads
static int delayedstart = 0; ///< Shall we start threads all at once or delayed

static int monitor_threads_len = 0; ///< only global because of signal handler
static pid_t *monitor_threads = NULL; ///< only global because of signal handler

static pthread_cond_t cond = PTHREAD_COND_INITIALIZER; ///< condition to broadcast all threads to begin at the same time
static pthread_mutex_t cond_m =  PTHREAD_MUTEX_INITIALIZER; ///< mutex to protect cond
static pthread_mutex_t sync_m =  PTHREAD_MUTEX_INITIALIZER; ///< mutex to protect sync_i
static volatile int sync_i; ///< number of worker threads remaining to start
static volatile int done = 0; ///< indicator of if we are stopping (0=no, 1=yes, timeout, 2=yes,trace complete)

/**
   Monitor host for a given period

   @param \a h host to monitor, \a ut, \a rt and \a dt added together gives to time to monitor
 */
static pid_t monitor_thr(string &h, long ut, long rt, long dt) {
     pid_t pid = fork();
     if (pid == 0) {
          int f = creat((h + ".monitor").c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
          if (f == -1)
               EABORT();
          dup2(f,fileno(stdout));
          dup2(f,fileno(stderr));
          close(f);
          char buf[20];
          sprintf(buf, "%lu", ut+rt+dt);
          cout << "Starting monitor on " << h << " with " << 
               sshpath << " " << h << " " << sarpath;
          for(int i = 0; i < 6; i++) {
               cout  << " " << saroptions[i];
          }
          cout << " " << buf << " > " << h << ".monitor" << endl;
          const char* argv[] = {
               sshpath,
               h.c_str(),
               sarpath,
               saroptions[0],
               saroptions[1],
               saroptions[2],
               saroptions[3],
               saroptions[4],
               saroptions[5],
               buf,
               NULL
          };
          
          execv(sshpath, (char**)argv);
          ABORT();
     }
     return pid;
}

/**
   Monitor host with mysql for a given period

   @param \a h host to monitor, \a ut, \a rt and \a dt added together gives to time to monitor
 */
static pid_t mymonitor_thr(string &h, long ut, long rt, long dt) {
     pid_t pid = fork();
     if (pid == 0) {
          int f = creat((h + ".mymonitor").c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
          if (f == -1)
               EABORT();
          dup2(f,fileno(stdout));
          dup2(f,fileno(stderr));
          close(f);
          char buf[20];
          sprintf(buf, "%lu", ut+rt+dt);
          const char* argv[] = {
               mysqlpath,
               "-u",
               user,
               "-s",
               "-h",
               h.c_str(),
               "-i",
               "1",
               "-c",
               buf,
               "processlist",
               NULL
          };
          
          execv(mysqlpath, (char**)argv);
          ABORT();
     }
     return pid;
}


/**
   Worker thread start function

   @param \a resultparam is a resultset_t*
 */
static void* start_new(void* resultparam) {
     resultset_t* res = (resultset_t*) resultparam;
     srand(res->seed);

     //hack to disable libmysqlclient's debug since it spends almost 25% of total running time
     extern int _no_db_;
     _no_db_ = 1;

     MYSQL dbase;
     mysql_init(&dbase);
     ABORTIF(pthread_mutex_lock(&sync_m));
     
     if (!mysql_real_connect(&dbase, host, user, pass, database, 0, mysqlsock, 0)) {
         cout << mysql_error(&dbase) << endl;
         MSGABORT("Connection failed to database");
     }

     cout << "." << flush;

     if (!delayedstart) {
          //     cout << "Starting " << getpid() << endl;
//          ABORTIF(pthread_mutex_lock(&sync_m));
          sync_i--;
          ABORTIF(pthread_mutex_lock(&cond_m));
          ABORTIF(pthread_mutex_unlock(&sync_m));
          ABORTIF(pthread_cond_wait(&cond, &cond_m));
          ABORTIF(pthread_mutex_unlock(&cond_m));
          //cout << "GO GO GO " << getpid() << endl;
     }
     else {
          ABORTIF(pthread_mutex_unlock(&sync_m));
     }

     class SQLGenerator gen(&dbase);

     mysql_autocommit(&dbase, 0);

     while (true) {

          struct timeval t_start, t_end;
          bool pending = 0;
          int sleeptime;
          while(true) {
               const struct aquery* q = gen.getnext(res, &sleeptime);
               if (!sleeptime)
                    break;
               int row = 0;

               //catch uncompleted transactions
               if (pending && gen.last_stm_was_new_tid())
                    mysql_rollback(&dbase);

               MYSQL_RES  *result = 0;
               gettimeofday(&t_start, NULL);
               switch (q->t) {
               case BEGIN:
                    // mysql doc says it is an implicit commit
                    pending = 0;
                    if (mysql_query(&dbase, "begin"))
                         MABORT();
                    break;
               case COMMIT:
                    pending = 0;
                    mysql_commit(&dbase);
                    break;
               case ROLLBACK:
                    pending = 0;
                    mysql_rollback(&dbase);
                    break;
               case SELECT:
                    pending = 1;
                    // XU: execute the query directly or prepared
                    if(0) {
                        if (mysql_query(&dbase, q->q.c_str())) {
                             MABORT();
                        }
                        result = mysql_store_result(&dbase);
                        MYSQL_ROW srow;
                        while ((srow = mysql_fetch_row(result)))
                             row++;
                        mysql_free_result(result);
                    } else {
                        //cout << q->q.c_str();
                        static MYSQL_STMT *stmt[10] = { NULL, NULL, NULL, NULL,
                                                        NULL, NULL, NULL, NULL,
                                                        NULL, NULL };
                        int id = res->clientid;
                        if (!stmt[id]) {
                            stmt[id] = mysql_prepare(&dbase, q->q.c_str(), strlen(q->q.c_str()));
                        }
                        if (!stmt[id]) {
                            MABORT();
                        }
                        assert(1 == mysql_param_count(stmt[id]));
                        int int_data = rand();
                        //cout << int_data << endl;
                        MYSQL_BIND param_bind;
                        param_bind.buffer_type = MYSQL_TYPE_LONG;
                        param_bind.buffer = (char*) & int_data;
                        param_bind.is_null = 0;
                        param_bind.length = 0;
                        if (mysql_bind_param(stmt[id], &param_bind)) {
                            MABORT();
                        }
                        result = mysql_get_metadata(stmt[id]);
                        if(!result) {
                            MABORT();
                        }
                        int column_count = mysql_num_fields(result);
                        assert(column_count == 4);
                        if (mysql_execute(stmt[id])) {
                            MABORT();
                        }
                        MYSQL_BIND result_bind[4];
                        int result_data[4];
                        unsigned long length[4];
                        my_bool is_null[4];
                        for(int i = 0; i < 4; i++) {
                            result_bind[i].buffer_type = MYSQL_TYPE_LONG;
                            result_bind[i].buffer = (char *) &result_data[i];
                            result_bind[i].is_null = &is_null[i];
                            result_bind[i].length = &length[i];
                        }
                        if(mysql_bind_result(stmt[id], result_bind)) {
                            MABORT();
                        }
                        if(mysql_stmt_store_result(stmt[id])) {
                            MABORT();
                        }
                        while (!mysql_fetch(stmt[id])) {
                             row++;
                        }
                        mysql_free_result(result);
/* no close for reusing it
                        if (mysql_stmt_close(stmt[id])) {
                            MABORT();
                        }
*/
                    }

                    break;
               case TEMPTPL:
                    pending = 1;
                    if (mysql_query(&dbase, q->q.c_str()))
                         MABORT();
                    result = mysql_store_result(&dbase);
                    mysql_free_result(result);
                    break;
               case WRITE:
                    pending = 1;
                    if (allowwrite) {
                         if (mysql_query(&dbase, q->q.c_str()))
                              MABORT();
                         result = mysql_store_result(&dbase);
                         mysql_free_result(result);
                    }
                    break;
               }

               gettimeofday(&t_end, NULL);
               res->update(new struct result(q, dbase.last_used_con->host, t_start, t_end));

               if (sleeptime != -1)
                    usleep(sleeptime);
               if (done == 1)
                    break;
          } //for queries

          if (pending)
               mysql_rollback(&dbase);
          if (!repeatlog && !done)
               done = 2;
          if (done)
               break;
          gen.reinit();
     }
     mysql_close(&dbase);
     return (void*)res;
}

/**
   Calculate the timediff of \a now and \a last and store it in \a res

   @param res result
   @param now a struct timeval just taken
   @param last some time in the past
*/
static inline
void gettimediffs(struct timeval& res, struct timeval &now, struct timeval &last) {
     res.tv_usec = now.tv_usec - last.tv_usec;
     res.tv_sec = now.tv_sec - last.tv_sec;
     if (res.tv_usec < 0) {
          res.tv_sec-=1;
          res.tv_usec+=1000000;
     }
}

/**
   Our signal handler, which is being used to catch SIGTERM and SIGINT with.

   @param signal The signal that has been catched.
*/
static void signal_handler(int signal)
{
     switch (signal) {
     case SIGPIPE:
          break;
     case SIGABRT:
     case SIGSEGV:
          if (monitor_threads) {
               for (int i = 0; i < monitor_threads_len; i++) {
                    kill(monitor_threads[i], SIGTERM);
               }
          }
          break;
     case SIGTERM:
     case SIGINT:
     case SIGQUIT:
          done = 1;
          break;
     case SIGHUP:
          ///\todo set flag to reread config
          break;
     case SIGUSR1:
          ///\todo set flag to reread config and flush everything
          break;
     default:
          ;
     }
}

int main(int argc, char** argv) {
     unsigned int seed = time(NULL);
     char pathname[2048];
     if (argv[0][0] != '/') {
          getcwd(pathname, 2048 - 128);
          strcat(pathname, "/");
          if (argv[0][0] == '.' && argv[0][1] == '/')
               strcat(pathname, argv[0]+2);
          else
               strcat(pathname, argv[0]);
     }
     else {
          strcpy(pathname, argv[0]);
     }

     char *fname = rindex(pathname, '/');
     if (fname)
          *fname ='\0';

     long rampuptime = 0;
     long runtime = 0;
     long rampdowntime = 0;

     vector<string> monitor_hosts;
     char* outputdir=NULL;
     for (int i = 1; i < argc; i++) {
          if (strcmp(argv[i], "-t") == 0)
               NRTHR = atoi(argv[++i]);
          else if (strcmp(argv[i], "--thread") == 0)
               NRTHR = atoi(argv[++i]);
          else if (strcmp(argv[i], "--trace") == 0)
               tracefile = argv[++i];
          else if (strcmp(argv[i], "--repeat") == 0)
               repeatlog = 1;
          else if (strcmp(argv[i], "--write") == 0)
               allowwrite = 1;
          else if (strcmp(argv[i], "-s") == 0)
               sleeptimeg = atoi(argv[++i])*1000;
          else if (strcmp(argv[i], "--sleep") == 0)
               sleeptimeg = atoi(argv[++i])*1000;
          else if (strcmp(argv[i], "--seed") == 0)
               seed = atoi(argv[++i]);
          else if (strcmp(argv[i], "--host") == 0)
               host = argv[++i];
          else if (strcmp(argv[i], "--user") == 0)
               user = argv[++i];
          else if (strcmp(argv[i], "--dstart") == 0)
               delayedstart = 1;
          else if (strcmp(argv[i], "--pass") == 0)
               pass = argv[++i];
          else if (strcmp(argv[i], "--database") == 0)
               database = argv[++i];
          else if (strcmp(argv[i], "--monitor") == 0) {
               char * tmp = strdup(argv[++i]);
               char* tok = strtok(tmp, ":");
               while(tok) {
                    monitor_hosts.push_back(string(tok));
                    tok = strtok(NULL, ":");
               }
               free(tmp);
          }
          else if (i == argc - 4) {
               rampuptime = atol(argv[i]);
          }
          else if (i == argc - 3) {
               runtime = atol(argv[i]);
          }
          else if (i == argc - 2) {
               rampdowntime = atol(argv[i]);
          }
          else if (i == argc - 1) {
               outputdir = argv[i];
          }
          else {
               cout << argv[0] << " - Unknown param " << argv[i] << endl;
               break;
          }
     }
     sync_i = NRTHR;

     if (!rampuptime || !runtime || !rampdowntime || !outputdir || !monitor_hosts.size()) {
          cout << "Need at least rampuptime runtime rampdown in sec a outputdir and something to monitor (disabled by now)" << endl;
          cout << "Usage: " << argv[0] << " rampup runtime rampdown output_dir" << endl;
          cout << rampuptime << " " << runtime << " " << rampdowntime << " " << outputdir << " " << monitor_hosts.size() << endl;
          exit(1);
     }

     char* oldtrace = tracefile;
     tracefile = (char*)malloc(strlen(pathname) + 50);
     strcpy(tracefile, pathname);
     strcat(tracefile,  "/");
     strcat(tracefile,  oldtrace);


     if (mkdir(outputdir, S_IRWXU | S_IRWXG | S_IRWXO) == -1 && errno != EEXIST)
          EABORT();

     if (chdir(outputdir) == -1)
          EABORT();

     errno=0;
     struct sigaction sa;
     memset(&sa, 0 , sizeof(sa));
     sa.sa_handler = signal_handler;
     ERR(sigaction(SIGPIPE, &sa, 0), "Error setting signal handler");
     ERR(sigaction(SIGABRT, &sa, 0), "Error setting signal handler");
     ERR(sigaction(SIGSEGV, &sa, 0), "Error setting signal handler");
     ERR(sigaction(SIGTERM, &sa, 0), "Error setting signal handler");
     ERR(sigaction(SIGINT, &sa, 0), "Error setting signal handler");
     ERR(sigaction(SIGQUIT, &sa, 0), "Error setting signal handler");
     ERR(sigaction(SIGHUP, &sa, 0), "Error setting signal handler");
     ERR(sigaction(SIGUSR1, &sa, 0), "Error setting signal handler");

     ofstream logfile("params.log", ios::out | ios::trunc);
     logfile << "nr_threads: " << NRTHR << endl;
     logfile << "repeat: " << repeatlog << endl;
     logfile << "using delayed start: " << delayedstart << endl;
     logfile << "using writes: " << allowwrite << endl;
     {
          char temp_buf[15];
          snprintf(temp_buf, 15, "%d", sleeptimeg);
          logfile << "using sleep: " << (sleeptimeg == -1 ? "Not sleeping" :
                                         ( sleeptimeg ? temp_buf
                                           : "using tpcw thinktime" )) << endl;
     }
     logfile << "seed: " << seed << endl;
     for (unsigned int i = 0; i < monitor_hosts.size(); i++)
          logfile <<  "monitor: " <<  monitor_hosts[i].c_str() << endl;
     logfile << "db_host: " << host << endl;
     logfile << "db_user: " << user << endl;
     logfile << "db: " << database << endl;
     logfile << "rampuptime: " << rampuptime << endl;
     logfile << "runtime: " << runtime << endl;
     logfile << "rampdowntime: " << rampdowntime << endl;

     pthread_t threads[NRTHR];
     vector<int> starttimes(NRTHR);
     if (delayedstart) {
          srand(seed);
          for (int i = 0; i < NRTHR; i++)
               starttimes[i] = static_cast<int>(1000.0*rampuptime/2*rand()/(RAND_MAX+1.0));
          sort(starttimes.begin(), starttimes.end());
     }
     else {
          for (int i = 0; i < NRTHR; i++) {
               resultset_t* res = new resultset_t(i);
               res->seed = seed + i + 1;
               int status = pthread_create(&threads[i], NULL, start_new, res);
               ABORTIF(status);
          }

          while(true) {
               ABORTIF(pthread_mutex_lock(&sync_m));
               if (!sync_i || done)
                    break;
               ABORTIF(pthread_mutex_unlock(&sync_m));
               usleep(1000);
          }
          ABORTIF(pthread_mutex_unlock(&sync_m));
     }

#if 0
     //all slave threads are now waiting for us to signal start if not
     //using delayed start, so lets spawn some monitor threads. If we
     //are using delayed start, then start monitoring now and start
     //the threads later
     monitor_threads_len = 2*monitor_hosts.size();
     monitor_threads = new pid_t[monitor_threads_len];
     for (int i = 0, j=0; i < monitor_threads_len; i++, j++) {
          monitor_threads[i] = monitor_thr(monitor_hosts[j], rampuptime, runtime, rampdowntime);
          if (monitor_threads[i] == -1) EABORT();
          i++;
          monitor_threads[i] = mymonitor_thr(monitor_hosts[j], rampuptime, runtime, rampdowntime);
          if (monitor_threads[i] == -1) EABORT();
     }
#endif

     if (!delayedstart) {
          ABORTIF(pthread_mutex_lock(&cond_m));
          ABORTIF(pthread_cond_broadcast(&cond));
          ABORTIF(pthread_mutex_unlock(&cond_m));
     }

     cout << "Starting test" << endl;
     //cout << "sleeping " << rampuptime << " milliseconds" << endl;
     if (delayedstart) {
          struct timeval ts,tn;
          int thr = 0;
          gettimeofday(&ts, NULL);
          for (vector<int>::const_iterator it = starttimes.begin(); it != starttimes.end(); ++it) {
               gettimeofday(&tn, NULL);
               gettimediffs(tn, tn, ts);
               int t = tn.tv_sec*1000 + tn.tv_usec/1000;
               // if starttime is longer away than 100 msec, sleep it away
               if (*it > t + 100)
                    usleep((*it - t) * 1000);
               if (done)
                    goto early_finish;
               resultset_t* res = new resultset_t(thr);
               res->seed = seed + thr + 1;
               int status = pthread_create(&threads[thr], NULL, start_new, res);
               ABORTIF(status);
               thr++;
          }
          //sleep the rest of rampuptime
          gettimeofday(&tn, NULL);
          gettimediffs(tn, tn, ts);
          usleep(tn.tv_sec*1000000 + tn.tv_usec);
     }
     else {
          sleep(rampuptime);
          if (done)
               goto early_finish;
     }
     cout << "rampup finished" << endl;

     rampupdone = 1;

     // We can recieve a SIGCLD, so that our sleep is interrupted
     //cout << "sleeping " << runtime << " milliseconds" << endl;
     sleep(runtime);
     if (done)
          goto early_finish;
     cout << "running finished" << endl;
     //cout << "sleeping " << rampdowntime << " milliseconds" << endl;
     sleep(rampdowntime);
     if (done)
          goto early_finish;
     cout << "rampdown finished" << endl;
early_finish:
     if (done) {
          cout << "Early finish" << endl;
          logfile << "Early finish" << endl;
          for (int i = 0; i < monitor_threads_len; i++) {
               kill(monitor_threads[i], SIGHUP);
          }
     }
     done = 1;

     for (int i = 0; i < monitor_threads_len; i++) {
          wait(NULL);
     }

     cout << "Waiting for threads to finish" << endl;
     ofstream qfile("queries", ios::out | ios::trunc);
     for (int i = 0; i < NRTHR; i++) {
          resultset_t* res;
          int status = pthread_join(threads[i], (void**)&res);
          ABORTIF(status);
          qfile << *res << endl;
     }
     qfile.close();

     cout << "Last tid requested " << gtid << endl;
     logfile << "Last tid requested " << gtid << endl;

     if (delayedstart) {
          int thr = 0;
          for (vector<int>::const_iterator it = starttimes.begin(); it != starttimes.end(); ++it)
               logfile << "Thread " << thr++ << " started at " << *it << " msec" << endl;
     }

     logfile.close();

     cout << "Plot graphs with the command: " << endl;
     cout << pathname << "/gen_graphs.sh " << outputdir << endl;

     return 0;
}
