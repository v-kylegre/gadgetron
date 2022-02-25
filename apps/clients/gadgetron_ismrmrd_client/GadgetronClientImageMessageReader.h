#pragma once

#include <memory>
#include <utility>

#include <ismrmrd/ismrmrd.h>
#include <ismrmrd/meta.h>
#include "ismrmrd/dataset.h"
#include <ismrmrd/xml.h>

#include "GadgetronClientMessageReader.h"

using namespace ISMRMRD;

class GadgetronClientImageMessageReader : public GadgetronClientMessageReader
{

public:
    GadgetronClientImageMessageReader(std::string filename, std::string groupname, std::unique_ptr<std::unique_lock<std::mutex>> ismrmrd_lock);
    ~GadgetronClientImageMessageReader();

    template <typename T>
    void read_data_attrib(boost::asio::ip::tcp::socket* stream, const ISMRMRD::ImageHeader& h, ISMRMRD::Image<T>& im);

    virtual void read(boost::asio::ip::tcp::socket* stream);

protected:
    std::string group_name_;
    std::string file_name_;
    std::unique_ptr<std::unique_lock<std::mutex>> ismrmrd_lock_;
    std::shared_ptr<ISMRMRD::Dataset> dataset_;
};
