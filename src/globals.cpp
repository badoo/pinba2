#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/nmsg_ticker.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct pinba_globals_impl_t : public pinba_globals_t
{
	pinba_globals_impl_t(pinba_options_t *options)
	{
		ticker_ = meow::make_unique<nmsg_ticker___single_thread_t>();
		dictionary_ = meow::make_unique<dictionary_t>();
	}

	virtual void startup()
	{
		// TODO
	}

	virtual nmsg_ticker_t* ticker() const { return ticker_.get(); }
	virtual dictionary_t*  dictionary() const { return dictionary_.get(); }

private:
	std::unique_ptr<nmsg_ticker_t> ticker_;
	std::unique_ptr<dictionary_t>  dictionary_;
};

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_globals_ptr pinba_init(pinba_options_t *options)
{
	return meow::make_unique<pinba_globals_impl_t>(options);
}