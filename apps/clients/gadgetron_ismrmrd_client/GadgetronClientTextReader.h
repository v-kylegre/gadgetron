#pragma once

#include "GadgetronClientMessageReader.h"

class GadgetronClientTextReader : public GadgetronClientMessageReader
{

public:
    GadgetronClientTextReader();
    virtual ~GadgetronClientTextReader();

    virtual void read(boost::asio::ip::tcp::socket* stream);
};
