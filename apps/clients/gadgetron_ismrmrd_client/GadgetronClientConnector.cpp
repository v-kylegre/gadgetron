#include "GadgetronClientConnector.h"

#include "NHLBICompression.h"

#include "GadgetronClientException.h"
#include "GadgetronMessageDefs.h"

using namespace NHLBI;

#if defined GADGETRON_COMPRESSION_ZFP
namespace
{
    size_t compress_zfp_tolerance(float* in, size_t samples, size_t coils, double tolerance, char* buffer, size_t buf_size)
    {
        zfp_type type = zfp_type_float;
        zfp_field* field = NULL;
        zfp_stream* zfp = NULL;
        bitstream* stream = NULL;
        size_t zfpsize = 0;

        zfp = zfp_stream_open(NULL);
        field = zfp_field_alloc();

        zfp_field_set_pointer(field, in);

        zfp_field_set_type(field, type);
        zfp_field_set_size_2d(field, samples, coils);
        zfp_stream_set_accuracy(zfp, tolerance);

        if (zfp_stream_maximum_size(zfp, field) > buf_size) {
            zfp_field_free(field);
            zfp_stream_close(zfp);
            stream_close(stream);
            throw std::runtime_error("Insufficient buffer space for compression");
        }

        stream = stream_open(buffer, buf_size);
        if (!stream) {
            zfp_field_free(field);
            zfp_stream_close(zfp);
            stream_close(stream);
            throw std::runtime_error("Cannot open compressed stream");
        }
        zfp_stream_set_bit_stream(zfp, stream);

        if (!zfp_write_header(zfp, field, ZFP_HEADER_FULL)) {
            zfp_field_free(field);
            zfp_stream_close(zfp);
            stream_close(stream);
            throw std::runtime_error("Unable to write compression header to stream");
        }

        zfpsize = zfp_compress(zfp, field);
        if (zfpsize == 0) {
            zfp_field_free(field);
            zfp_stream_close(zfp);
            stream_close(stream);
            throw std::runtime_error("Compression failed");
        }

        zfp_field_free(field);
        zfp_stream_close(zfp);
        stream_close(stream);
        return zfpsize;
    }


    size_t compress_zfp_precision(float* in, size_t samples, size_t coils, unsigned int precision, char* buffer, size_t buf_size)
    {
    zfp_type type = zfp_type_float;
    zfp_field* field = NULL;
    zfp_stream* zfp = NULL;
    bitstream* stream = NULL;
    size_t zfpsize = 0;

    zfp = zfp_stream_open(NULL);
    field = zfp_field_alloc();

    zfp_field_set_pointer(field, in);

    zfp_field_set_type(field, type);
    zfp_field_set_size_2d(field, samples, coils);
    zfp_stream_set_precision(zfp, precision);

    if (zfp_stream_maximum_size(zfp, field) > buf_size) {
        zfp_field_free(field);
        zfp_stream_close(zfp);
        stream_close(stream);
        throw std::runtime_error("Insufficient buffer space for compression");
    }

    stream = stream_open(buffer, buf_size);
    if (!stream) {
        zfp_field_free(field);
        zfp_stream_close(zfp);
        stream_close(stream);
        throw std::runtime_error("Cannot open compressed stream");
    }
    zfp_stream_set_bit_stream(zfp, stream);

    if (!zfp_write_header(zfp, field, ZFP_HEADER_FULL)) {
        zfp_field_free(field);
        zfp_stream_close(zfp);
        stream_close(stream);
        throw std::runtime_error("Unable to write compression header to stream");
    }

    zfpsize = zfp_compress(zfp, field);
    if (zfpsize == 0) {
        zfp_field_free(field);
        zfp_stream_close(zfp);
        stream_close(stream);
        throw std::runtime_error("Compression failed");
    }

    zfp_field_free(field);
    zfp_stream_close(zfp);
    stream_close(stream);
    return zfpsize;
    }
}
#endif //GADGETRON_COMPRESSION_ZFP



GadgetronClientConnector::GadgetronClientConnector()
    : socket_(0)
    , timeout_ms_(10000)
    , uncompressed_bytes_sent_(0)
    , compressed_bytes_sent_(0)
    , header_bytes_sent_(0)
{

}

GadgetronClientConnector::~GadgetronClientConnector()
{
    if (socket_) {
        socket_->close();
        delete socket_;
    }
}

double GadgetronClientConnector::compression_ratio()
{
    if (compressed_bytes_sent_ <= 0) {
        return 1.0;
    }

    return uncompressed_bytes_sent_/compressed_bytes_sent_;
}

double GadgetronClientConnector::get_bytes_transmitted()
{
    if (compressed_bytes_sent_ <= 0) {
        return header_bytes_sent_ + uncompressed_bytes_sent_;
    } else {
        return header_bytes_sent_ + compressed_bytes_sent_;
    }
}

void GadgetronClientConnector::set_timeout(unsigned int t)
{
    timeout_ms_ = t;
}

void GadgetronClientConnector::read_task()
{
    if (!socket_) {
        throw GadgetronClientException("Unable to create socket.");
    }

    while (socket_->is_open()) {

        GadgetMessageIdentifier id;
        boost::asio::read(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));

        if (id.id == GADGET_MESSAGE_CLOSE) {
            break;
        }

        GadgetronClientMessageReader* r = find_reader(id.id);

        if (!r) {
            std::cout << "Message received with ID: " << id.id << std::endl;
            throw GadgetronClientException("Unknown Message ID");
        } else {
            r->read(socket_);
        }
    }
}

void GadgetronClientConnector::wait() {
    reader_thread_.join();
}

void GadgetronClientConnector::connect(std::string hostname, std::string port)
{
    boost::asio::ip::tcp::resolver resolver(io_service_);
    // numeric_service flag is required to send data if the Linux machine has no internet connection (in this case the loopback device is the only network device with an address).
    // https://stackoverflow.com/questions/5971242/how-does-boost-asios-hostname-resolution-work-on-linux-is-it-possible-to-use-n
    // https://www.boost.org/doc/libs/1_65_0/doc/html/boost_asio/reference/ip__basic_resolver_query.html

    boost::asio::ip::tcp::resolver::query query(
            boost::asio::ip::tcp::v4(),
            hostname.c_str(),
            port.c_str(),
            boost::asio::ip::resolver_query_base::numeric_service);

    boost::asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
    boost::asio::ip::tcp::resolver::iterator end;

    socket_ = new boost::asio::ip::tcp::socket(io_service_);
    if (!socket_) {
        throw GadgetronClientException("Unable to create socket.");
    }

    std::condition_variable cv;
    std::mutex cv_m;

    boost::system::error_code error = boost::asio::error::host_not_found;
    std::thread t([&](){
            //TODO:
            //For newer versions of Boost, we should use
            //   boost::asio::connect(*socket_, iterator);
            while (error && endpoint_iterator != end) {
                socket_->close();
                socket_->connect(*endpoint_iterator++, error);
            }
            cv.notify_all();
        });

    {
        std::unique_lock<std::mutex> lk(cv_m);
        if (std::cv_status::timeout == cv.wait_until(lk, std::chrono::system_clock::now() +std::chrono::milliseconds(timeout_ms_)) ) {
            socket_->close();
            }
    }

    t.join();
    if (error)
        throw GadgetronClientException("Error connecting using socket.");

    reader_thread_ = std::thread([&](){this->read_task();});
}

void GadgetronClientConnector::send_gadgetron_close() {
    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }
    GadgetMessageIdentifier id;
    id.id = GADGET_MESSAGE_CLOSE;
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
}

void GadgetronClientConnector::send_gadgetron_info_query(const std::string &query, uint64_t correlation_id) {
    GadgetMessageIdentifier id{ 6 }; // 6 = QUERY; Deal with it.

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(id)));

    uint64_t reserved = 0;

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&reserved, sizeof(reserved)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&correlation_id, sizeof(correlation_id)));

    uint64_t query_length = query.size();

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&query_length, sizeof(query_length)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(query));
}

void GadgetronClientConnector::send_gadgetron_configuration_file(std::string config_xml_name) {

    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    id.id = GADGET_MESSAGE_CONFIG_FILE;

    GadgetMessageConfigurationFile ini;
    memset(&ini,0,sizeof(GadgetMessageConfigurationFile));
    strncpy(ini.configuration_file, config_xml_name.c_str(),config_xml_name.size());

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&ini, sizeof(GadgetMessageConfigurationFile)));

}

void GadgetronClientConnector::send_gadgetron_configuration_script(std::string xml_string)
{
    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    id.id = GADGET_MESSAGE_CONFIG_SCRIPT;

    GadgetMessageScript conf;
    conf.script_length = (uint32_t)xml_string.size();

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&conf, sizeof(GadgetMessageScript)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(xml_string.c_str(), conf.script_length));
}

void GadgetronClientConnector::send_gadgetron_parameters(std::string xml_string)
{
    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    id.id = GADGET_MESSAGE_PARAMETER_SCRIPT;

    GadgetMessageScript conf;
    conf.script_length = (uint32_t)xml_string.size();

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&conf, sizeof(GadgetMessageScript)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(xml_string.c_str(), conf.script_length));
}

void GadgetronClientConnector::send_ismrmrd_acquisition(ISMRMRD::Acquisition& acq)
{
    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    id.id = GADGET_MESSAGE_ISMRMRD_ACQUISITION;;

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&acq.getHead(), sizeof(ISMRMRD::AcquisitionHeader)));

    unsigned long trajectory_elements = acq.getHead().trajectory_dimensions*acq.getHead().number_of_samples;
    unsigned long data_elements = acq.getHead().active_channels*acq.getHead().number_of_samples;

    if (trajectory_elements) {
        header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&acq.getTrajPtr()[0], sizeof(float)*trajectory_elements));
    }


    if (data_elements) {
        uncompressed_bytes_sent_ +=boost::asio::write(*socket_, boost::asio::buffer(&acq.getDataPtr()[0], 2*sizeof(float)*data_elements));
    }
}


void GadgetronClientConnector::send_ismrmrd_compressed_acquisition_precision(ISMRMRD::Acquisition& acq, unsigned int compression_precision)
{
    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    id.id = GADGET_MESSAGE_ISMRMRD_ACQUISITION;

    ISMRMRD::AcquisitionHeader h = acq.getHead(); //We will make a copy because we will be setting some flags
    h.setFlag(ISMRMRD::ISMRMRD_ACQ_COMPRESSION2);

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&h, sizeof(ISMRMRD::AcquisitionHeader)));

    unsigned long trajectory_elements = acq.getHead().trajectory_dimensions*acq.getHead().number_of_samples;
    unsigned long data_elements = acq.getHead().active_channels*acq.getHead().number_of_samples;

    if (trajectory_elements) {
        header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&acq.getTrajPtr()[0], sizeof(float)*trajectory_elements));
    }


    if (data_elements) {
        std::vector<float> input_data((float*)&acq.getDataPtr()[0], (float*)&acq.getDataPtr()[0] + acq.getHead().active_channels*acq.getHead().number_of_samples*2);

        std::unique_ptr<CompressedFloatBuffer> comp_buffer(CompressedFloatBuffer::createCompressedBuffer());
        comp_buffer->compress(input_data, -1.0, compression_precision);
        std::vector<uint8_t> serialized_buffer = comp_buffer->serialize();

        compressed_bytes_sent_ += serialized_buffer.size();
        uncompressed_bytes_sent_ += data_elements*2*sizeof(float);

        uint32_t bs = (uint32_t)serialized_buffer.size();
        boost::asio::write(*socket_, boost::asio::buffer(&bs, sizeof(uint32_t)));
        boost::asio::write(*socket_, boost::asio::buffer(&serialized_buffer[0], serialized_buffer.size()));
    }

}


void GadgetronClientConnector::send_ismrmrd_compressed_acquisition_tolerance(ISMRMRD::Acquisition& acq, float compression_tolerance, NoiseStatistics& stat)
{
    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    id.id = GADGET_MESSAGE_ISMRMRD_ACQUISITION;

    ISMRMRD::AcquisitionHeader h = acq.getHead(); //We will make a copy because we will be setting some flags
    h.setFlag(ISMRMRD::ISMRMRD_ACQ_COMPRESSION2);

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&h, sizeof(ISMRMRD::AcquisitionHeader)));

    unsigned long trajectory_elements = acq.getHead().trajectory_dimensions*acq.getHead().number_of_samples;
    unsigned long data_elements = acq.getHead().active_channels*acq.getHead().number_of_samples;

    if (trajectory_elements) {
        header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&acq.getTrajPtr()[0], sizeof(float)*trajectory_elements));
    }


    if (data_elements) {
        std::vector<float> input_data((float*)&acq.getDataPtr()[0], (float*)&acq.getDataPtr()[0] + acq.getHead().active_channels* acq.getHead().number_of_samples*2);

        float local_tolerance = compression_tolerance;
        float sigma = stat.sigma_min; //We use the minimum sigma of all channels to "cap" the error
        if (stat.status && sigma > 0 && stat.noise_dwell_time_us && acq.getHead().sample_time_us) {
            local_tolerance = local_tolerance*stat.sigma_min*acq.getHead().sample_time_us*std::sqrt(stat.noise_dwell_time_us/acq.getHead().sample_time_us);
        }

        std::unique_ptr<CompressedFloatBuffer> comp_buffer(CompressedFloatBuffer::createCompressedBuffer());
        comp_buffer->compress(input_data, local_tolerance);
        std::vector<uint8_t> serialized_buffer = comp_buffer->serialize();

        compressed_bytes_sent_ += serialized_buffer.size();
        uncompressed_bytes_sent_ += data_elements*2*sizeof(float);

        uint32_t bs = (uint32_t)serialized_buffer.size();
        boost::asio::write(*socket_, boost::asio::buffer(&bs, sizeof(uint32_t)));
        boost::asio::write(*socket_, boost::asio::buffer(&serialized_buffer[0], serialized_buffer.size()));
    }
}

void GadgetronClientConnector::send_ismrmrd_zfp_compressed_acquisition_precision(ISMRMRD::Acquisition& acq, unsigned int compression_precision)
{

#if defined GADGETRON_COMPRESSION_ZFP
    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    //TODO: switch data type
    id.id = GADGET_MESSAGE_ISMRMRD_ACQUISITION;

    ISMRMRD::AcquisitionHeader h = acq.getHead(); //We will make a copy because we will be setting some flags
    h.setFlag(ISMRMRD::ISMRMRD_ACQ_COMPRESSION1);

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&h, sizeof(ISMRMRD::AcquisitionHeader)));

    unsigned long trajectory_elements = acq.getHead().trajectory_dimensions*acq.getHead().number_of_samples;
    unsigned long data_elements = acq.getHead().active_channels*acq.getHead().number_of_samples;

    if (trajectory_elements) {
        header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&acq.getTrajPtr()[0], sizeof(float)*trajectory_elements));
    }


    if (data_elements) {
        size_t comp_buffer_size = 4*sizeof(float)*data_elements;
        char* comp_buffer = new char[comp_buffer_size];
        size_t compressed_size = 0;
        try {
            compressed_size = compress_zfp_precision((float*)&acq.getDataPtr()[0],
                                                        acq.getHead().number_of_samples*2, acq.getHead().active_channels,
                                                        compression_precision, comp_buffer, comp_buffer_size);

            compressed_bytes_sent_ += compressed_size;
            uncompressed_bytes_sent_ += data_elements*2*sizeof(float);
            float compression_ratio = (1.0*data_elements*2*sizeof(float))/(float)compressed_size;
            //std::cout << "Compression ratio: " << compression_ratio << std::endl;

        } catch (...) {
            delete [] comp_buffer;
            std::cout << "Compression failure caught" << std::endl;
            throw;
        }


        //TODO: Write compressed buffer
        uint32_t bs = (uint32_t)compressed_size;
        boost::asio::write(*socket_, boost::asio::buffer(&bs, sizeof(uint32_t)));
        boost::asio::write(*socket_, boost::asio::buffer(comp_buffer, compressed_size));

        delete [] comp_buffer;
    }

#else //GADGETRON_COMPRESSION_ZFP
    throw GadgetronClientException("Attempting to do ZFP compression, but ZFP not available");
#endif //GADGETRON_COMPRESSION_ZFP
}

void GadgetronClientConnector::send_ismrmrd_zfp_compressed_acquisition_tolerance(ISMRMRD::Acquisition& acq, float compression_tolerance, NoiseStatistics& stat)
{
#if defined GADGETRON_COMPRESSION_ZFP
    if (!socket_) {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    //TODO: switch data type
    id.id = GADGET_MESSAGE_ISMRMRD_ACQUISITION;

    ISMRMRD::AcquisitionHeader h = acq.getHead(); //We will make a copy because we will be setting some flags
    h.setFlag(ISMRMRD::ISMRMRD_ACQ_COMPRESSION1);

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&h, sizeof(ISMRMRD::AcquisitionHeader)));

    unsigned long trajectory_elements = acq.getHead().trajectory_dimensions*acq.getHead().number_of_samples;
    unsigned long data_elements = acq.getHead().active_channels*acq.getHead().number_of_samples;

    if (trajectory_elements) {
        header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&acq.getTrajPtr()[0], sizeof(float)*trajectory_elements));
    }

    float local_tolerance = compression_tolerance;
    float sigma = stat.sigma_min; //We use the minimum sigma of all channels to "cap" the error
    if (stat.status && sigma > 0 && stat.noise_dwell_time_us && acq.getHead().sample_time_us) {
        local_tolerance = local_tolerance*stat.sigma_min*acq.getHead().sample_time_us*std::sqrt(stat.noise_dwell_time_us/acq.getHead().sample_time_us);
    }

    if (data_elements) {
        size_t comp_buffer_size = 4*sizeof(float)*data_elements;
        char* comp_buffer = new char[comp_buffer_size];
        size_t compressed_size = 0;
        try {
            compressed_size = compress_zfp_tolerance((float*)&acq.getDataPtr()[0],
                                                        acq.getHead().number_of_samples*2, acq.getHead().active_channels,
                                                        local_tolerance, comp_buffer, comp_buffer_size);

            compressed_bytes_sent_ += compressed_size;
            uncompressed_bytes_sent_ += data_elements*2*sizeof(float);
            float compression_ratio = (1.0*data_elements*2*sizeof(float))/(float)compressed_size;
            //std::cout << "Compression ratio: " << compression_ratio << std::endl;

        } catch (...) {
            delete [] comp_buffer;
            std::cout << "Compression failure caught" << std::endl;
            throw;
        }


        //TODO: Write compressed buffer
        uint32_t bs = (uint32_t)compressed_size;
        boost::asio::write(*socket_, boost::asio::buffer(&bs, sizeof(uint32_t)));
        boost::asio::write(*socket_, boost::asio::buffer(comp_buffer, compressed_size));

        delete [] comp_buffer;
    }
#else //GADGETRON_COMPRESSION_ZFP
    throw GadgetronClientException("Attempting to do ZFP compression, but ZFP not available");
#endif //GADGETRON_COMPRESSION_ZFP

}

void GadgetronClientConnector::send_ismrmrd_waveform(ISMRMRD::Waveform& wav)
{
    if (!socket_)
    {
        throw GadgetronClientException("Invalid socket.");
    }

    GadgetMessageIdentifier id;
    id.id = GADGET_MESSAGE_ISMRMRD_WAVEFORM;;

    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&id, sizeof(GadgetMessageIdentifier)));
    header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(&wav.head, sizeof(ISMRMRD::ISMRMRD_WaveformHeader)));

    unsigned long data_elements = wav.head.channels*wav.head.number_of_samples;

    if (data_elements)
    {
        header_bytes_sent_ += boost::asio::write(*socket_, boost::asio::buffer(wav.begin_data(), sizeof(uint32_t)*data_elements));
    }
}

void GadgetronClientConnector::register_reader(unsigned short slot, std::shared_ptr<GadgetronClientMessageReader> r) {
    readers_[slot] = r;
}


GadgetronClientMessageReader* GadgetronClientConnector::find_reader(unsigned short r)
{
    GadgetronClientMessageReader* ret = 0;

    maptype::iterator it = readers_.find(r);

    if (it != readers_.end()) {
        ret = it->second.get();
    }

    return ret;
}
