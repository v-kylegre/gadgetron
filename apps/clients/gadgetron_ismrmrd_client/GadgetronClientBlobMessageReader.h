#pragma once

#include <boost/asio.hpp>

#include "GadgetronClientMessageReader.h"

class GadgetronClientBlobMessageReader : public GadgetronClientMessageReader
{

public:
    GadgetronClientBlobMessageReader(std::string fileprefix, std::string filesuffix);
    virtual ~GadgetronClientBlobMessageReader();

    virtual void read(boost::asio::ip::tcp::socket* socket);

protected:
    size_t number_of_calls_;
    std::string file_prefix_;
    std::string file_suffix_;
};
