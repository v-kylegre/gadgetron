#pragma once

#include "GadgetronClientMessageReader.h"

class GadgetronClientResponseReader : public GadgetronClientMessageReader
{
    void read(boost::asio::ip::tcp::socket *stream) override;
};
