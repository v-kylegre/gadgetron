/*****************************************
*  Standalone ISMRMRD Gadgetron Client  
*
* Author: Michael S. Hansen
* 
* Dependencies: ISMRMRD and Boost
*
*****************************************/

//TODO:
// -Blobs (for DICOM image support)
//  - First implementation is in, but testing needed
// -NIFTI and Analyze output
// -Check on potential threading problem with asio socket 
//    - having and reading and writing thread is supposedly not safe, but seems to work here
// -Static linking for standalone executable. 

#include <boost/program_options.hpp>
#include <boost/asio.hpp>

#include <ismrmrd/ismrmrd.h>
#include <ismrmrd/dataset.h>
#include <ismrmrd/meta.h>
#include <ismrmrd/xml.h>
#include <ismrmrd/waveform.h>

#include <streambuf>
#include <time.h>
#include <iomanip>
#include <iostream>
#include <exception>
#include <map>
#include <thread>
#include <chrono>
#include <condition_variable>

#include "NHLBICompression.h"
#include "GadgetronTimer.h"
#include "gadgetron_client_exception.h"

namespace po = boost::program_options;
using boost::asio::ip::tcp;


class GadgetronClientMessageReader
{
public:
    virtual ~GadgetronClientMessageReader() {}

    /**
    Function must be implemented to read a specific message.
    */
    virtual void read(tcp::socket* s) = 0;

};

class GadgetronClientResponseReader : public GadgetronClientMessageReader
{
    void read(tcp::socket *stream) override {

        uint64_t correlation_id = 0;
        uint64_t response_length = 0;

        boost::asio::read(*stream, boost::asio::buffer(&correlation_id, sizeof(correlation_id)));
        boost::asio::read(*stream, boost::asio::buffer(&response_length, sizeof(response_length)));

        std::vector<char> response(response_length + 1,0);

        boost::asio::read(*stream, boost::asio::buffer(response.data(),response_length));

        std::cout << response.data() << std::endl;
    }
};

class GadgetronClientTextReader : public GadgetronClientMessageReader
{
  
public:
  GadgetronClientTextReader()
  {
    
  }

  virtual ~GadgetronClientTextReader()
  {
    
  }

  virtual void read(tcp::socket* stream)
  {
    size_t recv_count = 0;
    
    typedef unsigned long long size_t_type;
    uint32_t len(0);
    boost::asio::read(*stream, boost::asio::buffer(&len, sizeof(uint32_t)));

    
    char* buf = NULL;
    try {
      buf = new char[len+1];
      memset(buf, '\0', len+1);
    } catch (std::runtime_error &err) {
      std::cerr << "TextReader, failed to allocate buffer" << std::endl;
      throw;
    }
       
    if (boost::asio::read(*stream, boost::asio::buffer(buf, len)) != len)
    {
      delete [] buf;
      throw GadgetronClientException("Incorrect number of bytes read for dependency query");  
    }

    std::string s(buf);
    std::cout << s;
    delete[] buf;
  }  
};


class GadgetronClientDependencyQueryReader : public GadgetronClientMessageReader
{
  
public:
  GadgetronClientDependencyQueryReader(std::string filename) : number_of_calls_(0) , filename_(filename)
  {
    
  }

  virtual ~GadgetronClientDependencyQueryReader()
  {
    
  }

  virtual void read(tcp::socket* stream)
  {
    size_t recv_count = 0;
    
    typedef unsigned long long size_t_type;
    size_t_type len(0);
    boost::asio::read(*stream, boost::asio::buffer(&len, sizeof(size_t_type)));

    char* buf = NULL;
    try {
      buf = new char[len];
      memset(buf, '\0', len);
      memcpy(buf, &len, sizeof(size_t_type));
    } catch (std::runtime_error &err) {
      std::cerr << "DependencyQueryReader, failed to allocate buffer" << std::endl;
      throw;
    }
    
    
    if (boost::asio::read(*stream, boost::asio::buffer(buf, len)) != len)
    {
      delete [] buf;
      throw GadgetronClientException("Incorrect number of bytes read for dependency query");  
    }
    
    std::ofstream outfile;
    outfile.open (filename_.c_str(), std::ios::out|std::ios::binary);

    if (outfile.good()) {
      outfile.write(buf, len);
      outfile.close();
      number_of_calls_++;
    } else {
      delete[] buf;
      throw GadgetronClientException("Unable to write dependency query to file");  
    }
    
    delete[] buf;
  }
  
  protected:
    size_t number_of_calls_;
    std::string filename_;
};


class GadgetronClientImageMessageReader : public GadgetronClientMessageReader
{

public:
    GadgetronClientImageMessageReader(std::string filename, std::string groupname, std::unique_ptr<std::unique_lock<std::mutex>> ismrmrd_lock)
        : file_name_(filename)
        , group_name_(groupname)
        , ismrmrd_lock_(std::move(ismrmrd_lock))
    {

    }

    ~GadgetronClientImageMessageReader() {
    } 

    template <typename T> 
    void read_data_attrib(tcp::socket* stream, const ISMRMRD::ImageHeader& h, ISMRMRD::Image<T>& im)
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

    virtual void read(tcp::socket* stream) 
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

protected:
    std::string group_name_;
    std::string file_name_;
    std::unique_ptr<std::unique_lock<std::mutex>> ismrmrd_lock_;
    std::shared_ptr<ISMRMRD::Dataset> dataset_;
};

// ----------------------------------------------------------------
// for the analyze image format
#ifdef DT_UNKNOWN
    #undef DT_UNKNOWN
#endif // DT_UNKNOWN

enum AnalyzeDataType
{
    DT_ANA_UNKNOWN=0,

    DT_NONE                    =0,
    DT_UNKNOWN                 =0,     /* what it says, dude           */
    DT_BINARY                  =1,     /* binary (1 bit/voxel)         */
    DT_UNSIGNED_CHAR           =2,     /* unsigned char (8 bits/voxel) */
    DT_SIGNED_SHORT            =4,     /* signed short (16 bits/voxel) */
    DT_UNSIGNED_SHORT          =5,
    DT_SIGNED_INT              =8,     /* signed int (32 bits/voxel)   */
    DT_UNSIGNED_INT            =9,
    DT_FLOAT                  =16,     /* float (32 bits/voxel)        */
    DT_COMPLEX                =32,     /* complex (64 bits/voxel)      */
    DT_DOUBLE                 =64,     /* double (64 bits/voxel)       */
    DT_RGB                   =128,     /* RGB triple (24 bits/voxel)   */
    DT_ALL                   =255,     /* not very useful (?)          */

                                /*----- another set of names for the same ---*/
    DT_UINT8                   =2,
    DT_INT16                   =4,
    DT_INT32                   =8,
    DT_FLOAT32                =16,
    DT_COMPLEX64              =32,
    DT_FLOAT64                =64,
    DT_RGB24                 =128,

                                /*------------------- new codes for NIFTI ---*/
    DT_INT8                  =256,     /* signed char (8 bits)         */
    DT_UINT16                =512,     /* unsigned short (16 bits)     */
    DT_UINT32                =768,     /* unsigned int (32 bits)       */
    DT_INT64                =1024,     /* long long (64 bits)          */
    DT_UINT64               =1280,     /* unsigned long long (64 bits) */
    DT_FLOAT128             =1536,     /* long double (128 bits)       */
    DT_COMPLEX128           =1792,     /* double pair (128 bits)       */
    DT_COMPLEX256           =2048,     /* long double pair (256 bits)  */
    DT_RGBA32               =2304,     /* 4 byte RGBA (32 bits/voxel)  */
};

AnalyzeDataType getDataTypeFromRTTI(const std::string& name)
{
    AnalyzeDataType analyzeDT = DT_ANA_UNKNOWN;

    if ( name == typeid(unsigned char).name() )
    {
        analyzeDT = DT_UNSIGNED_CHAR;
    }

    if ( name == typeid(short).name() )
    {
        analyzeDT = DT_SIGNED_SHORT;
    }

    if ( name == typeid(unsigned short).name() )
    {
        analyzeDT = DT_UINT16;
    }

    if ( name == typeid(int).name() )
    {
        analyzeDT = DT_SIGNED_INT;
    }

    if ( name == typeid(unsigned int).name() )
    {
        analyzeDT = DT_UINT32;
    }

    if ( name == typeid(float).name() )
    {
        analyzeDT = DT_FLOAT;
    }

    if ( name == typeid(double).name() )
    {
        analyzeDT = DT_DOUBLE;
    }

    if ( name == typeid(long double).name() )
    {
        analyzeDT = DT_FLOAT128;
    }

    if ( name == typeid(std::complex<float>).name() )
    {
        analyzeDT = DT_COMPLEX;
    }

    if ( name == typeid(std::complex<double>).name() )
    {
        analyzeDT = DT_COMPLEX128;
    }

    if ( name == typeid(std::complex<long double>).name() )
    {
        analyzeDT = DT_COMPLEX256;
    }

    return analyzeDT;
}

struct header_key
{
    int sizeof_hdr;
    char data_type[10];
    char db_name[18];
    int extents;
    short int session_error;
    char regular;
    char hkey_un0;
};

struct image_dimension
{
    short int dim[8];
    short int unused8;
    short int unused9;
    short int unused10;
    short int unused11;
    short int unused12;
    short int unused13;
    short int unused14;
    short int datatype;
    short int bitpix;
    short int dim_un0;
    float pixdim[8];
    float vox_offset;
    float funused1;
    float funused2;
    float funused3;
    float cal_max;
    float cal_min;
    float compressed;
    float verified;
    int glmax,glmin;
};

struct data_history
{
    char descrip[80];
    char aux_file[24];
    char orient;
    char originator[10];
    char generated[10];
    char scannum[10];
    char patient_id[10];
    char exp_date[10];
    char exp_time[10];
    char hist_un0[3];
    int views;
    int vols_added;
    int start_field;
    int field_skip;
    int omax, omin;
    int smax, smin;
};

// Analyze75 header has 348 bytes
struct dsr
{
    struct header_key hk;
    struct image_dimension dime;
    struct data_history hist;
};

class IOAnalyze
{
public:

    typedef dsr HeaderType;

    IOAnalyze() {}
    virtual ~IOAnalyze() {}

    template <typename T> void array2Header(const std::vector<size_t>& dim, const std::vector<float>& pixelSize, HeaderType& header)
    {
        try
        {
            // set everything to zero
            memset(&header, 0, sizeof(dsr));

            // header_key
            header.hk.sizeof_hdr = 348;
            size_t i;
            for (i=0; i<10; i++ ) header.hk.data_type[i] = 0;
            for (i=0; i<18; i++ ) header.hk.db_name[i] = 0;
            header.hk.extents = 16384;
            header.hk.session_error = 0;
            header.hk.regular = 'r';
            header.hk.hkey_un0 = 0;

            // image_dimension
            size_t NDim = dim.size();

            header.dime.dim[0] = (short)(NDim);
            header.dime.dim[1] = (short)(dim[0]);

            if ( NDim > 1 )
                header.dime.dim[2] = (short)(dim[1]);
            else
                header.dime.dim[2] = 1;

            if ( NDim > 2 )
                header.dime.dim[3] = (short)(dim[2]);
            else
                header.dime.dim[3] = 1;

            if ( NDim > 3 )
                header.dime.dim[4] = (short)(dim[3]);
            else
                header.dime.dim[4] = 1;

            if ( NDim > 4 )
                header.dime.dim[5] = (short)(dim[4]);
            else
                header.dime.dim[5] = 1;

            if ( NDim > 5 )
                header.dime.dim[6] = (short)(dim[5]);
            else
                header.dime.dim[6] = 1;

            if ( NDim > 6 )
                header.dime.dim[7] = (short)(dim[6]);
            else
                header.dime.dim[7] = 1;

            if ( NDim > 7 )
                header.dime.unused8 = (short)(dim[7]);
            else
                header.dime.unused8 = 1;

            if ( NDim > 8 )
                header.dime.unused9 = (short)(dim[8]);
            else
                header.dime.unused9 = 1;

            if ( NDim > 9 )
                header.dime.unused10 = (short)(dim[9]);
            else
                header.dime.unused10 = 1;

            header.dime.unused11 = 0;
            header.dime.unused12 = 0;
            header.dime.unused13 = 0;
            header.dime.unused14 = 0;

            std::string rttiID = std::string(typeid(T).name());
            header.dime.datatype = (short)getDataTypeFromRTTI(rttiID);
            header.dime.bitpix = (short)(8*sizeof(T));
            header.dime.dim_un0 = 0;

            // since the NDArray does not carry the pixel spacing
            header.dime.pixdim[0] = 0;
            if ( pixelSize.size() > 0 )
                header.dime.pixdim[1] = pixelSize[0];
            if ( pixelSize.size() > 1 )
                header.dime.pixdim[2] = pixelSize[1];
            if ( pixelSize.size() > 2 )
                header.dime.pixdim[3] = pixelSize[2];
            if ( pixelSize.size() > 3 )
                header.dime.pixdim[4] = pixelSize[3];
            if ( pixelSize.size() > 4 )
                header.dime.pixdim[5] = pixelSize[4];
            if ( pixelSize.size() > 5 )
                header.dime.pixdim[6] = pixelSize[5];
            if ( pixelSize.size() > 6 )
                header.dime.pixdim[7] = pixelSize[6];

            header.dime.vox_offset = 0;
            header.dime.funused1 = 0;
            header.dime.funused2 = 0;
            header.dime.funused3 = 0;
            header.dime.cal_max = 0;
            header.dime.cal_min = 0;
            header.dime.compressed = 0;
            header.dime.verified = 0;
            header.dime.glmax = 0;
            header.dime.glmin = 0;

            // data history
            for (i=0; i<80; i++ ) header.hist.descrip[i] = 0;
            for (i=0; i<24; i++ ) header.hist.aux_file[i] = 0;
            header.hist.orient = 0;
            for (i=0; i<10; i++ ) header.hist.originator[i] = 0;
            for (i=0; i<10; i++ ) header.hist.generated[i] = 0;
            for (i=0; i<10; i++ ) header.hist.scannum[i] = 0;
            for (i=0; i<10; i++ ) header.hist.patient_id[i] = 0;
            for (i=0; i<10; i++ ) header.hist.exp_date[i] = 0;
            for (i=0; i<10; i++ ) header.hist.exp_time[i] = 0;
            for (i=0; i<3; i++ ) header.hist.hist_un0[i] = 0;
            header.hist.views = 0;
            header.hist.vols_added = 0;
            header.hist.start_field = 0;
            header.hist.field_skip = 0;
            header.hist.omax = 0;
            header.hist.omin = 0;
            header.hist.smax = 0;
            header.hist.smin = 0;
        }
        catch(...)
        {
            throw GadgetronClientException("Errors in IOAnalyze::array2Analyze(dim, header) ... ");
        }
    }
};

class GadgetronClientAnalyzeImageMessageReader : public GadgetronClientMessageReader
{

public:

    GadgetronClientAnalyzeImageMessageReader(const std::string& prefix = std::string("Image")) : prefix_(prefix)
    {

    }

    ~GadgetronClientAnalyzeImageMessageReader() {
    } 

    template <typename T>
    void read_data_attrib(tcp::socket* stream, const ISMRMRD::ImageHeader& h, ISMRMRD::Image<T>& im)
    {
        im.setHead(h);

        std::cout << "Receiving image : " << h.image_series_index << " - " << h.image_index << std::endl;

        typedef unsigned long long size_t_type;

        std::ostringstream ostr;

        if (!prefix_.empty())
        {
            ostr << prefix_ << "_";
        }

        ostr << "SLC" << h.slice << "_"
            << "CON" << h.contrast << "_"
            << "PHS" << h.phase << "_"
            << "REP" << h.repetition << "_"
            << "SET" << h.set << "_"
            << "AVE" << h.average << "_"
            << h.image_index
            << "_" << h.image_series_index;

        std::string filename = ostr.str();

        //Read meta attributes
        size_t_type meta_attrib_length;
        boost::asio::read(*stream, boost::asio::buffer(&meta_attrib_length, sizeof(size_t_type)));

        if (meta_attrib_length > 0)
        {
            std::string meta_attrib(meta_attrib_length, 0);
            boost::asio::read(*stream, boost::asio::buffer(const_cast<char*>(meta_attrib.c_str()), meta_attrib_length));

            // deserialize the meta attribute
            ISMRMRD::MetaContainer imgAttrib;
            ISMRMRD::deserialize(meta_attrib.c_str(), imgAttrib);

            std::stringstream st3;
            st3 << filename << ".attrib";
            std::string meta_varname = st3.str();

            std::ofstream outfile;
            outfile.open(meta_varname.c_str(), std::ios::out | std::ios::binary);
            outfile.write(meta_attrib.c_str(), meta_attrib_length);
            outfile.close();
        }

        //Read data
        boost::asio::read(*stream, boost::asio::buffer(im.getDataPtr(), im.getDataSize()));

        // analyze header
        std::stringstream st1;
        st1 << filename << ".hdr";
        std::string head_varname = st1.str();

        std::vector<size_t> dim(4, 1);
        dim[0] = h.matrix_size[0];
        dim[1] = h.matrix_size[1];
        dim[2] = h.matrix_size[2];
        dim[3] = h.channels;

        std::vector<float> pixelSize(4, 1);
        pixelSize[0] = h.field_of_view[0] / h.matrix_size[0];
        pixelSize[1] = h.field_of_view[1] / h.matrix_size[1];
        pixelSize[2] = h.field_of_view[2] / h.matrix_size[2];

        IOAnalyze hdr;
        dsr header;
        hdr.array2Header<T>(dim, pixelSize, header);

        std::ofstream outfileHeader;
        outfileHeader.open(head_varname.c_str(), std::ios::out | std::ios::binary);
        outfileHeader.write(reinterpret_cast<const char*>(&header), sizeof(dsr));
        outfileHeader.close();

        // data
        std::stringstream st2;
        st2 << filename << ".img";
        std::string img_varname = st2.str();

        std::ofstream outfileData;
        outfileData.open(img_varname.c_str(), std::ios::out | std::ios::binary);
        outfileData.write(reinterpret_cast<const char*>(im.getDataPtr()), sizeof(T)*dim[0] * dim[1] * dim[2] * dim[3]);
        outfileData.close();
    }

    virtual void read(tcp::socket* stream) 
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

protected:

    std::string prefix_;
};

// ----------------------------------------------------------------

#define MAX_BLOBS_LOG_10    6

class GadgetronClientBlobMessageReader 
    : public GadgetronClientMessageReader
{

public:
    GadgetronClientBlobMessageReader(std::string fileprefix, std::string filesuffix)
        : number_of_calls_(0)
        , file_prefix(fileprefix)
        , file_suffix(filesuffix)

    {

    }

    virtual ~GadgetronClientBlobMessageReader() {}

    virtual void read(tcp::socket* socket) 
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
        filename << file_prefix << "_";
        filename << std::setfill('0') << std::setw(MAX_BLOBS_LOG_10) << number_of_calls_;
        filename_attrib = filename.str();
        filename << "." << file_suffix;
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

protected:
    size_t number_of_calls_;
    std::string file_prefix;
    std::string file_suffix;

};

class GadgetronClientQueryToStringReader : public GadgetronClientMessageReader
{
  
public:
  GadgetronClientQueryToStringReader(std::string& result) : result_(result)
  {
    
  }

  virtual ~GadgetronClientQueryToStringReader()
  {
    
  }

  virtual void read(tcp::socket* stream)
  {
    size_t recv_count = 0;
    
    typedef unsigned long long size_t_type;
    size_t_type len(0);
    boost::asio::read(*stream, boost::asio::buffer(&len, sizeof(size_t_type)));

    std::vector<char> temp(len,0);
    if (boost::asio::read(*stream, boost::asio::buffer(temp.data(), len)) != len)
    {
      throw GadgetronClientException("Incorrect number of bytes read for dependency query");
    }
    result_ = std::string(temp.data(),len);
    
  }
  
  protected:
    std::string& result_;
};
