#include "GadgetronClientImageMessageReader.h"

#include "GadgetronClientException.h"

using namespace ISMRMRD;

GadgetronClientImageMessageReader::GadgetronClientImageMessageReader(std::string filename, std::string groupname, std::unique_ptr<std::unique_lock<std::mutex>> ismrmrd_lock)
    : file_name_(filename)
    , group_name_(groupname)
    , ismrmrd_lock_(std::move(ismrmrd_lock))
{
}

GadgetronClientImageMessageReader::~GadgetronClientImageMessageReader()
{
}

template <typename T>
void GadgetronClientImageMessageReader::read_data_attrib(boost::asio::ip::tcp::socket* stream, const ISMRMRD::ImageHeader& h, ISMRMRD::Image<T>& im)
{
    im.setHead(h);

    typedef unsigned long long size_t_type;

    //Read meta attributes
    size_t_type meta_attrib_length;
    boost::asio::read(*stream, boost::asio::buffer(&meta_attrib_length, sizeof(size_t_type)));

    if (meta_attrib_length>0)
    {
        std::string meta_attrib(meta_attrib_length, 0);
        boost::asio::read(*stream, boost::asio::buffer(const_cast<char*>(meta_attrib.c_str()), meta_attrib_length));
        im.setAttributeString(meta_attrib);
    }

    //Read image data
    boost::asio::read(*stream, boost::asio::buffer(im.getDataPtr(), im.getDataSize()));
    {
        if (!dataset_) {

            {
                ismrmrd_lock_->lock();
                dataset_ = std::shared_ptr<ISMRMRD::Dataset>(new ISMRMRD::Dataset(file_name_.c_str(), group_name_.c_str(), true)); // create if necessary
                ismrmrd_lock_->unlock();
            }
        }

        std::stringstream st1;
        st1 << "image_" << h.image_series_index;
        std::string image_varname = st1.str();

        {
            ismrmrd_lock_->lock();
            //TODO should this be wrapped in a try/catch?
            dataset_->appendImage(image_varname, im);
            ismrmrd_lock_->unlock();
        }
    }
}

void GadgetronClientImageMessageReader::read(boost::asio::ip::tcp::socket* stream)
{
    //Read the image headerfrom the socket
    ISMRMRD::ImageHeader h;
    boost::asio::read(*stream, boost::asio::buffer(&h,sizeof(ISMRMRD::ImageHeader)));

    if (h.data_type == ISMRMRD::ISMRMRD_USHORT)
    {
        ISMRMRD::Image<unsigned short> im;
        this->read_data_attrib(stream, h, im);
    }
    else if (h.data_type == ISMRMRD::ISMRMRD_SHORT)
    {
        ISMRMRD::Image<short> im;
        this->read_data_attrib(stream, h, im);
    }
    else if (h.data_type == ISMRMRD::ISMRMRD_UINT)
    {
        ISMRMRD::Image<unsigned int> im;
        this->read_data_attrib(stream, h, im);
    }
    else if (h.data_type == ISMRMRD::ISMRMRD_INT)
    {
        ISMRMRD::Image<int> im;
        this->read_data_attrib(stream, h, im);
    }
    else if (h.data_type == ISMRMRD::ISMRMRD_FLOAT)
    {
        ISMRMRD::Image<float> im;
        this->read_data_attrib(stream, h, im);
    }
    else if (h.data_type == ISMRMRD::ISMRMRD_DOUBLE)
    {
        ISMRMRD::Image<double> im;
        this->read_data_attrib(stream, h, im);
    }
    else if (h.data_type == ISMRMRD::ISMRMRD_CXFLOAT)
    {
        ISMRMRD::Image< std::complex<float> > im;
        this->read_data_attrib(stream, h, im);
    }
    else if (h.data_type == ISMRMRD::ISMRMRD_CXDOUBLE)
    {
        ISMRMRD::Image< std::complex<double> > im;
        this->read_data_attrib(stream, h, im);
    }
    else
    {
        throw GadgetronClientException("Invalid image data type ... ");
    }
}
