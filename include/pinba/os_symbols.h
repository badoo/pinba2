#ifndef PINBA__OS_SYMBOLS_H_
#define PINBA__OS_SYMBOLS_H_

#include <sys/types.h>
#include <sys/socket.h>

#include "pinba/globals.h"

////////////////////////////////////////////////////////////////////////////////////////////////
// declare required types, for functions that we want to runtime-check

#ifndef PINBA_HAVE_RECVMMSG

	struct mmsghdr {
		struct msghdr msg_hdr;  /* Message header */
		unsigned int  msg_len;  /* Number of received bytes for header */
	};

#endif

////////////////////////////////////////////////////////////////////////////////////////////////

struct pinba_os_symbols_t : private boost::noncopyable
{
	virtual ~pinba_os_symbols_t() {}

	// generic dlsym
	virtual void* resolve(str_ref) = 0;

	// wrappers for specific functions
	using funcp___pthread_setname_np_t = int (*)(pthread_t thread, const char *name);
	virtual int set_thread_name(str_ref) = 0;

	using funcp___pthread_setaffinity_np_t = int (*)(pthread_t thread, size_t cpusetsize, const cpu_set_t *cpuset);
	virtual int set_thread_affinity(size_t cpusetsize, const cpu_set_t *cpuset) = 0;

	using funcp___recvmmsg_t = int (*)(int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags, const struct timespec *timeout);
	virtual int  recvmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags, const struct timespec *timeout) = 0;
	virtual bool has_recvmmsg() const = 0;
};
using pinba_os_symbols_ptr = std::unique_ptr<pinba_os_symbols_t>;

pinba_os_symbols_ptr pinba_os_symbols___init(pinba_globals_t*);


#define PINBA___OS_CALL(g, func_name, ...)   \
	g->os_symbols()->func_name(__VA_ARGS__); \
/**/

// dl a function, cast it to a given type and call with args
// PINBA___DL_AND_CALL(globals_, "pthread_setname_np", void(*)(char const*), conf_.thread_name.c_str());
#define PINBA___OS_RESOLVE_AND_CALL(g, func_name, func_type, ...)       \
	[&]() {                                                             \
		auto const f = (func_type)g->os_symbols()->resolve(func_name);  \
		return f(__VA_ARGS__);                                          \
	}();                                                                \
/**/

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__OS_SYMBOLS_H_
