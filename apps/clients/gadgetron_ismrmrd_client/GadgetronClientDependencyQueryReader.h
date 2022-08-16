
#pragma once

#include <boost/asio.hpp>

#include "GadgetronClientMessageReader.h"

class GadgetronClientDependencyQueryReader : public GadgetronClientMessageReader
{

public:
    GadgetronClientDependencyQueryReader(std::string filename);
    virtual ~GadgetronClientDependencyQueryReader();

    virtual void read(boost::asio::ip::tcp::socket* stream);

private:
        size_t number_of_calls_;
        std::string filename_;
};
