#include "pinba_config.h"

#include <dlfcn.h>

#include "pinba/globals.h"
#include "pinba/os_symbols.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	struct pinba_os_symbols_impl_t : public pinba_os_symbols_t
	{
		pinba_os_symbols_impl_t(pinba_globals_t *globals)
			: globals_(globals)
		{
			dl_self_ = dlopen(NULL, RTLD_LAZY);
			if (dl_self_ == NULL)
				throw std::runtime_error(ff::fmt_str("dlopen(self): {0}", dlerror()));

			this->resolve_builtin_symbols();
		}

		virtual ~pinba_os_symbols_impl_t()
		{
			if (dl_self_)
				dlclose(dl_self_);
		}

		virtual void* resolve(str_ref name) override
		{
			assert(dl_self_ != NULL);

			void *fp     = NULL;
			char *errmsg = NULL;
			{
				// i suppose, that dl*() functions are not thread-safe at all, given their global nature
				// so lock here to be safe, let clients cache stuff
				std::unique_lock<std::mutex> lk_(dl_mtx_);

				// clear any errors that were in
				dlerror();

				fp = dlsym(dl_self_, name.str().c_str());
				errmsg = dlerror();
			}

			if (fp == NULL && errmsg != NULL)
				throw std::runtime_error(ff::fmt_str("dlsym(self, '{0}'): {1}", name, errmsg));

			LOG_INFO(globals_->logger(), "dlsym resolved function '{0}'... {1}", name, (fp) ? "OK" : "not found");

			return fp;
		}

		virtual int set_thread_name(str_ref name) override
		{
			if (!fp_pthread_setname_np_)
				return 0;

			return fp_pthread_setname_np_(pthread_self(), name.str().c_str());
		}

		virtual int set_thread_affinity(size_t cpusetsize, const cpu_set_t *cpuset) override
		{
			if (!fp_pthread_setaffinity_np_)
				return 0;

			return fp_pthread_setaffinity_np_(pthread_self(), cpusetsize, cpuset);
		}

		virtual int recvmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags, const struct timespec *timeout) override
		{
			assert(fp_recvmmsg_ != NULL);
			return fp_recvmmsg_(sockfd, msgvec, vlen, flags, timeout);
		}

		virtual bool has_recvmmsg() const override
		{
			return (fp_recvmmsg_ != NULL);
		}

	private:

		void resolve_builtin_symbols()
		{
			#ifdef PINBA_HAVE_PTHREAD_SETNAME_NP
				fp_pthread_setname_np_     = (funcp___pthread_setname_np_t)&pthread_setname_np;
			#else
				fp_pthread_setname_np_     = (funcp___pthread_setname_np_t)this->resolve("pthread_setname_np");
			#endif

			#ifdef PINBA_HAVE_PTHREAD_SETAFFINITY_NP
				fp_pthread_setaffinity_np_ = (funcp___pthread_setaffinity_np_t)&pthread_setaffinity_np;
			#else
				fp_pthread_setaffinity_np_ = (funcp___pthread_setaffinity_np_t)this->resolve("pthread_setaffinity_np");
			#endif

			#ifdef PINBA_HAVE_RECVMMSG
				fp_recvmmsg_               = (funcp___recvmmsg_t)&::recvmmsg;
			#else
				fp_recvmmsg_               = (funcp___recvmmsg_t)this->resolve("recvmmsg");
			#endif
		}

	private:
		pinba_globals_t  *globals_;
		void             *dl_self_;
		std::mutex       dl_mtx_;

		funcp___pthread_setname_np_t      fp_pthread_setname_np_;
		funcp___pthread_setaffinity_np_t  fp_pthread_setaffinity_np_;
		funcp___recvmmsg_t                fp_recvmmsg_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

pinba_os_symbols_ptr pinba_os_symbols___init(pinba_globals_t *globals)
try
{
	return meow::make_unique<aux::pinba_os_symbols_impl_t>(globals);
}
catch (std::exception const& e)
{
	throw std::runtime_error(ff::fmt_str("{0}; {1}", __func__, e.what()));
}
