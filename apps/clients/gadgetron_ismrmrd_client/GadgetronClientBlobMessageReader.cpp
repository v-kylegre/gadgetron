#include "GadgetronClientBlobMessageReader.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "GadgetronClientException.h"

namespace
{
    const int MAX_BLOBS_LOG_10 = 6;
}

GadgetronClientBlobMessageReader::GadgetronClientBlobMessageReader(std::string fileprefix, std::string filesuffix)
    : number_of_calls_(0)
    , file_prefix_(fileprefix)
    , file_suffix_(filesuffix)
{

}

GadgetronClientBlobMessageReader::~GadgetronClientBlobMessageReader()
{

}

void GadgetronClientBlobMessageReader::read(boost::asio::ip::tcp::socket* socket)
{

    // MUST READ 32-bits
    uint32_t nbytes;
    boost::asio::read(*socket, boost::asio::buffer(&nbytes,sizeof(uint32_t)));

    std::vector<char> data(nbytes,0);
    boost::asio::read(*socket, boost::asio::buffer(&data[0],nbytes));

    unsigned long long fileNameLen;
    boost::asio::read(*socket, boost::asio::buffer(&fileNameLen,sizeof(unsigned long long)));

    std::string filenameBuf(fileNameLen,0);
    boost::asio::read(*socket, boost::asio::buffer(const_cast<char*>(filenameBuf.c_str()),fileNameLen));

    typedef unsigned long long size_t_type;

    size_t_type meta_attrib_length;
    boost::asio::read(*socket, boost::asio::buffer(&meta_attrib_length, sizeof(size_t_type)));

    std::string meta_attrib;
    if (meta_attrib_length > 0)
    {
        std::string meta_attrib_socket(meta_attrib_length, 0);
        boost::asio::read(*socket, boost::asio::buffer(const_cast<char*>(meta_attrib_socket.c_str()), meta_attrib_length));
        meta_attrib = meta_attrib_socket;
    }

    std::stringstream filename;
    std::string filename_attrib;

    // Create the filename: (prefix_%06.suffix)
    filename << file_prefix_ << "_";
    filename << std::setfill('0') << std::setw(MAX_BLOBS_LOG_10) << number_of_calls_;
    filename_attrib = filename.str();
    filename << "." << file_suffix_;
    filename_attrib.append("_attrib.xml");

    std::cout << "Writing image " << filename.str() << std::endl;

    std::ofstream outfile;
    outfile.open(filename.str().c_str(), std::ios::out | std::ios::binary);

    std::ofstream outfile_attrib;
    if (meta_attrib_length > 0)
    {
        outfile_attrib.open(filename_attrib.c_str(), std::ios::out | std::ios::binary);
    }

    if (outfile.good())
    {
        /* write 'size' bytes starting at 'data's pointer */
        outfile.write(&data[0], nbytes);
        outfile.close();

        if (meta_attrib_length > 0)
        {
            outfile_attrib.write(meta_attrib.c_str(), meta_attrib.length());
            outfile_attrib.close();
        }

        number_of_calls_++;
    }
    else
    {
        throw GadgetronClientException("Unable to write blob to output file\n");
    }
}
