#pragma once

#include "GadgetronClientMessageReader.h"

class GadgetronClientQueryToStringReader : public GadgetronClientMessageReader
{

public:
  GadgetronClientQueryToStringReader(std::string& result);
  virtual ~GadgetronClientQueryToStringReader();

  virtual void read(boost::asio::ip::tcp::socket* stream);

  protected:
    std::string& result_;
};
