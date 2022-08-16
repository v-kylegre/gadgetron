#pragma once

#include <boost/asio.hpp>

class GadgetronClientMessageReader
{
public:
    virtual ~GadgetronClientMessageReader() {}

    /**
    Function must be implemented to read a specific message.
    */
    virtual void read(boost::asio::ip::tcp::socket* s) = 0;

};
