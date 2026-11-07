#include <iostream>
#include <cerrno>
#include <fstream>
#include <thread>
#include <chrono>
#include <mutex>
#include <cstdint>
#include <unistd.h>
#include <fcntl.h>
#include <filesystem>
#include <signal.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <zmq_addon.hpp>
#include <atomic>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <limits>
#include <csignal>
#include <queue>
#include <array>
#include <condition_variable>

#include "queue.hpp"
#include "dma-proxy.h"
#include "argparse.hpp"

// TODO
// - define a max size for file event cache

#define EVENT_CACHE_FILE      "/tmp/event.cache"
#define ETH0_TX_BYTES_FILE    "/sys/class/net/eth0/statistics/tx_bytes"
#define MAX_FLUSH_SLEEP_US    2000000
#define MAX_FLUSH_BANDWITH    80    // Mbit/s
#define FLUSH_LATENCY_US      2800

// command line arguments
bool debug = false;
bool local = false;
std::string host;
uint16_t port;
uint16_t mpmtid;
uint16_t size;
std::string mode;
bool runcontrol;

// DMA
const std::string rx_channel_name = "dma_proxy_rx";

struct channel {
   struct channel_buffer *buf_ptr;
   int fd;
   pthread_t tid;
};

struct channel rx_channel;

// ZMQ
zmq::context_t ctx;

// run control
// std::string run_state;
// static volatile bool keep_running = true;

//state variables
std::atomic<bool> acquisition_running{false};
std::atomic<bool> workers_running{false};
std::atomic<bool> program_running{true};

//signal handler
volatile std::sig_atomic_t shutdown_requested = 0;

enum class ProducerCommand {
   START,
   STOP,
   RESET
};

struct PendingCommand {
   ProducerCommand command;
   std::string request_id;
};

struct ProducerReply {
   ProducerCommand command;
   std::string request_id;
   bool success;
   std::string error;
};

std::queue<PendingCommand> pending_commands;
std::queue<ProducerReply> completed_commands;
std::mutex reply_mutex;
std::mutex command_mutex;

std::mutex dma_state_mutex;
std::condition_variable dma_state_changed;
bool dma_submitted = false;
bool dma_failed = false;
std::atomic<bool> dma_enabled{true};
std::atomic<bool> dma_idle{true};
std::atomic<uint64_t> completed_not_sent{0};


// queues
TSQueue<uint16_t> q1, q2, q3;

// cache file
std::string fname;
std::atomic<uint64_t> cached_buffers = 0;
std::mutex fmtx;              // event cache mutex
std::mutex smtx;              // ZMQ socket mutex
std::atomic<bool> flush_allowed{false};

// ZMQ transfer mutex
zmq::socket_t sock;

// bandwith monitor
uint64_t tx_bandwith;                            // bytes/s
std::atomic<uint64_t> tx_bandwith_mbps{0};       // mbit/s
std::atomic<uint16_t> tx_flush_bandwith_mbps{0};

// flush control
//std::mutex tmtx;
std::atomic<uint32_t> flush_sleep_us{MAX_FLUSH_SLEEP_US};


struct DataThreads {
   std::thread dma;
   std::thread tx;
   std::thread cache;
   std::thread flush;
};

void intHandler(int) {
   shutdown_requested  = 1;
}

void set_dma_result(bool success){
   {
   std::lock_guard<std::mutex> lock(dma_state_mutex);
   if(success) dma_submitted = true;
   else dma_failed = true;
   }

   dma_state_changed.notify_all();
}

const char* command_name(ProducerCommand command) {
   switch (command) {
   case ProducerCommand::START: return "start";
   case ProducerCommand::STOP: return "stop";
   case ProducerCommand::RESET: return "reset";
   }
   return "unknown";
}


void enqueue_command(ProducerCommand command, const std::string& request_id = ""){
   std::lock_guard<std::mutex> lock(command_mutex);
   pending_commands.push({command, request_id});
}


bool take_command(PendingCommand& command){
   std::lock_guard<std::mutex> lock(command_mutex);

   if (pending_commands.empty())
      return false;

   command = pending_commands.front();
   pending_commands.pop();

   return true;

}

// run control THREAD
auto control = [](std::string thread_id) {

   zmq::socket_t control_socket(ctx, zmq::socket_type::sub);
   const std::string url = "tcp://" + host + ":4444";
   control_socket.set(zmq::sockopt::subscribe, "control");
   control_socket.set(zmq::sockopt::subscribe, std::to_string(mpmtid) + "|");
   control_socket.set(zmq::sockopt::rcvtimeo, 100);
   control_socket.set(zmq::sockopt::linger, 0);
   control_socket.connect(url);

   zmq::socket_t reply_socket(ctx, zmq::socket_type::push);
   const std::string url_reply = "tcp://" + host + ":4445";
   reply_socket.set(zmq::sockopt::immediate, 1);
   reply_socket.set(zmq::sockopt::sndtimeo, 1000);
   reply_socket.set(zmq::sockopt::linger, 0);
   reply_socket.connect(url_reply);
   

   std::cout << "I: control thread subscribed to " << url << std::endl;
   std::cout << "I: reply socket connected  to " << url_reply << std::endl;

   while(program_running.load()) {
      try{
         while(true){
            ProducerReply reply;
            {
               std::lock_guard<std::mutex> lock(reply_mutex);
               if(completed_commands.empty()) break;
               reply = completed_commands.front();
               completed_commands.pop();
            }
            const std::string source = std::to_string(mpmtid);
            const std::string operation = command_name(reply.command);
            const std::string status = reply.success ? "OK" : "ERROR";
            std::array<zmq::const_buffer, 5> frames = {
               zmq::buffer(source), zmq::buffer(reply.request_id),
               zmq::buffer(operation), zmq::buffer(status), zmq::buffer(reply.error)
            };
            if (!zmq::send_multipart(reply_socket, frames)){
               std::cerr << "Cannot send ACK for " << reply.request_id << std::endl;
            }
         }
         zmq::message_t topic;
         zmq::message_t payload;

         if (!control_socket.recv(topic))
            continue;

         if (!control_socket.get(zmq::sockopt::rcvmore))
            continue;

         if (!control_socket.recv(payload))
            continue;

         bool extra_parts = false;
         while(control_socket.get(zmq::sockopt::rcvmore)){
            extra_parts = true;
            zmq::message_t extra;
            if (!control_socket.recv(extra))
               break;

         }

         if (extra_parts)
            continue;
         
         const std::string command = payload.to_string();
         const std::string topic_text = topic.to_string();
         const auto separator = topic_text.find('|');
         const std::string request_id = separator == std::string::npos ? "" : topic_text.substr(separator+1);
         if (request_id.size() >= 64) continue;

         if(command  == "start"){
            enqueue_command(ProducerCommand::START, request_id);
         }
         else if(command == "stop"){
            enqueue_command(ProducerCommand::STOP, request_id);
         }
         else if(command == "reset"){
            enqueue_command(ProducerCommand::RESET, request_id);
         }
         else{
            std::cerr << thread_id << ": unknown command: " << command << std::endl;
         }

         std::cout << "I: " << payload.to_string_view() << std::endl;

      } catch (const zmq::error_t& error){
         if (error.num() == EINTR && !program_running.load()) continue;
         std::cerr << thread_id << ": " << error.what() << std::endl;
      }

      //std::this_thread::sleep_for(std::chrono::milliseconds(500));
   }
};

// DMA transfer THREAD
auto dma_transfer = [](std::string thread_id) {

   int32_t buffer_id;      // int32_t due to dma-proxy ioctl prototype: _IOW('a', 'b', int32_t *)
   uint16_t free_buffer_id;

   proxy_status status;
   bool next_xfer = true;

   // initialize DMA buffers area
   //memset(rx_channel.buf_ptr, 0, sizeof(struct channel_buffer));

   if (!q1.pop(free_buffer_id))
      return;
   
   buffer_id = free_buffer_id;

   while(workers_running.load() && dma_enabled.load()) {

      if(acquisition_running.load()) {

         dma_idle.store(false);
         rx_channel.buf_ptr[buffer_id].length = size;                  
         if(next_xfer){
            auto& buffer = rx_channel.buf_ptr[buffer_id];
            if (size == 0 || size > sizeof(buffer.buffer)){
               std::cerr << thread_id << ": invalid DMA transfer size: " << size << std::endl;
               acquisition_running.store(false);
               set_dma_result(false);
               dma_idle.store(true);
               return;
            }

            if (debug) {
               std::memset(buffer.buffer, 0, size);

               if (buffer_id == 0) {
                     std::cout
                        << thread_id
                        << ": DEBUG: buffer 0 cleared before START_XFER, "
                        << size << " bytes"
                        << std::endl;
               }
            }

            buffer.length = size;
            
            if (ioctl(rx_channel.fd, START_XFER, &buffer_id) < 0){
               perror("START_XFER");
               acquisition_running.store(false);
               set_dma_result(false);
               dma_idle.store(true);
               return;
            }
            
            set_dma_result(true); // descriptor submitted; no packet needed yet
            next_xfer = false;
         }
         
         if (ioctl(rx_channel.fd, FINISH_XFER, &buffer_id) < 0){
            perror("FINISH_XFER");
            acquisition_running.store(false);
            set_dma_result(false);
            dma_idle.store(true);
            return;
         }

         if (!workers_running.load())
            return;


         status = rx_channel.buf_ptr[buffer_id].status;

         if(debug)
            std::cout << thread_id << ": buffer " << buffer_id << " transfer completed - status: " << 
               status << " transferred_length: " << rx_channel.buf_ptr[buffer_id].transferred_length << std::endl;

         if (status == PROXY_NO_ERROR) {

            next_xfer = true;

            if (debug) {
               const uint8_t* buf =
                  reinterpret_cast<const uint8_t*>(
                        rx_channel.buf_ptr[buffer_id].buffer
                  );

               unsigned int dump_length =
                  rx_channel.buf_ptr[buffer_id].transferred_length;

               if (dump_length > 48)
                  dump_length = 48;

               printf(
                  "DMA buffer %d: first %u bytes\n",
                  static_cast<int>(buffer_id),
                  dump_length
               );

               for (unsigned int i = 0; i < dump_length; ++i) {
                  printf(
                        "%02X ",
                        static_cast<unsigned int>(buf[i])
                  );

                  if ((i + 1) % 16 == 0)
                        printf("\n");
               }

               if (dump_length % 16 != 0)
                  printf("\n");
            }
            
            if (mode != "local") completed_not_sent.fetch_add(1);
            if(mode == "local")        // in local mode reinsert completed buffer id
               q1.push(static_cast<uint16_t>(buffer_id));
            else                       // in remote mode send completed buffer id to zmq_transfer thread
               q2.push(static_cast<uint16_t>(buffer_id));

            if (!q1.pop(free_buffer_id))
               return;

            buffer_id = free_buffer_id;

         } else if (status == PROXY_BUSY || status == PROXY_TIMEOUT) {
            next_xfer = false;
         } else{
            set_dma_result(false);
            acquisition_running.store(false);
            dma_idle.store(true);
            return;
         }
         
         dma_idle.store(true);

      } else std::this_thread::sleep_for(std::chrono::milliseconds(50));
   }
};

// ZMQ transfer THREAD
auto zmq_transfer = [](std::string thread_id) {

   uint16_t buffer_id;

   while(workers_running.load()) {

      if(acquisition_running.load()) {

         if (!q2.pop(buffer_id))
            return;

         if (!workers_running.load())
            return;

         zmq::const_buffer buf = zmq::buffer(rx_channel.buf_ptr[buffer_id].buffer, rx_channel.buf_ptr[buffer_id].transferred_length);

         smtx.lock();
            auto rc = sock.send(buf, zmq::send_flags::dontwait);
         smtx.unlock();

         /*
          * rc holding the return of a send(), a zmq::send_result_t 
          * that is a std::optional which may hold a size_t giving 
          * the number of bytes sent or nothing if EAGAIN error occurred
         */

         if(rc.has_value()) {
            
            if(rc.value() != rx_channel.buf_ptr[buffer_id].transferred_length) {          // send failed (partial send)

               std::cout << thread_id << ": partial buffer sent " << rc.value() << std::endl;

            } else {       // send success
               completed_not_sent.fetch_sub(1);
               q1.push(buffer_id);
            }

         } else {

            if(errno == EAGAIN)                                   // send failed (bandwith full)....
               q3.push(buffer_id);                                // wake-up cache thread...
         }

      } else std::this_thread::sleep_for(std::chrono::milliseconds(50));
   }
};

// DISK cache THREAD
auto disk_cache = [](std::string thread_id) {

   uint16_t buffer_id;
   char *bufchar;

   std::ofstream evcache;
   evcache = std::ofstream(fname, std::ios::out | std::ios::binary);

   while (workers_running.load()) {

      if(acquisition_running.load()) {

         if (!q3.pop(buffer_id))
            return;

         if (!workers_running.load())
            return;

         bufchar = reinterpret_cast<char *>(rx_channel.buf_ptr[buffer_id].buffer);

         fmtx.lock();
            evcache.seekp(0, evcache.end);
            evcache.write(bufchar, rx_channel.buf_ptr[buffer_id].length);
            evcache.flush();     // commit changes
            cached_buffers++;
         fmtx.unlock();

         q1.push(buffer_id);
          
      } else std::this_thread::sleep_for(std::chrono::milliseconds(50));
   }
};

// DISK flush THREAD
auto disk_flush = [](std::string thread_id) {

   char *cbuf; 
   uint64_t flush_tx_bytes, prev_flush_tx_bytes;
   std::chrono::steady_clock::time_point start;

   std::ifstream evcache;
   prev_flush_tx_bytes = flush_tx_bytes = 0;

   cbuf = new char[BUFFER_SIZE];

   start = std::chrono::steady_clock::now();

   while (workers_running.load()) {

      // update stats
      std::chrono::steady_clock::duration time_elapsed = std::chrono::steady_clock::now() - start;
      long time_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_elapsed).count();

      if(time_elapsed_ms >= 1000) {

         tx_flush_bandwith_mbps = ((flush_tx_bytes - prev_flush_tx_bytes) / float(time_elapsed_ms / 1000.0)) * 8 / 1024 / 1024;

         // due to TCP buffering application level bandwith can be greater than maximum...
         tx_flush_bandwith_mbps = std::min((uint16_t)100, tx_flush_bandwith_mbps.load());

         start = std::chrono::steady_clock::now();
         prev_flush_tx_bytes = flush_tx_bytes;
      }

      if(acquisition_running.load()) {

retry:   
         if (!workers_running.load()){
            break;
         }

         if (!acquisition_running.load()){
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
         }

         flush_allowed = (q3.size() == 0);

         if(cached_buffers > 0) {

            if(!evcache.is_open())
               evcache.open(fname, std::ios::binary);

            std::this_thread::sleep_for(std::chrono::microseconds(flush_sleep_us.load()));

            if(flush_allowed) {

               if (!workers_running.load())
                  break;

               if (!acquisition_running.load())
                  continue;

               const int64_t avail_bandwith = int64_t(MAX_FLUSH_BANDWITH) - static_cast<int64_t>(tx_bandwith_mbps.load()) + static_cast<int64_t>(tx_flush_bandwith_mbps.load());

               if(avail_bandwith <= 0) {

                  //tmtx.lock();
                     flush_sleep_us = MAX_FLUSH_SLEEP_US;
                  //tmtx.unlock();
                  goto retry;

               } else {

                  const double available_bytes_per_second = static_cast<double>(avail_bandwith) * 1024.0 * 1024.0 / 8.0;
                  const double calculated_sleep_us = double(BUFFER_SIZE) / available_bytes_per_second * 1.0e6  - FLUSH_LATENCY_US;
                  flush_sleep_us.store(static_cast<uint32_t>(std::clamp(calculated_sleep_us, 0.0, double(MAX_FLUSH_SLEEP_US))));
                  //float value = ((1.0 / (avail_bandwith_bs/BUFFER_SIZE)) * 1000 * 1000) - FLUSH_LATENCY_US;
                  //tmtx.lock();
                     //flush_sleep_us = value;
                  //tmtx.unlock();
               }

               fmtx.lock();
                  evcache.sync();
                  evcache.seekg(0, evcache.end);
                  evcache.seekg(-BUFFER_SIZE, evcache.cur);
                  evcache.read(cbuf, BUFFER_SIZE);

                  zmq::message_t msg(cbuf, BUFFER_SIZE);

                  smtx.lock();
                     auto rc = sock.send(std::move(msg), zmq::send_flags::dontwait);
                  smtx.unlock();

                  if(rc.has_value()) {

                     if(rc.value() != BUFFER_SIZE) {       // send failed

                        std::cout << thread_id << ": partial buffer sent " << rc.value() << std::endl;

                     } else {    // send success

                        // update stats
                        flush_tx_bytes += BUFFER_SIZE;
                        completed_not_sent.fetch_sub(1);

                        // resize event cache file...
                        std::uintmax_t fsize = std::filesystem::file_size(fname);
                        std::uintmax_t newfsize;

                        if(fsize < BUFFER_SIZE) {
                           newfsize = 0;
                           cached_buffers -= fsize / BUFFER_SIZE;
                        } else {
                           newfsize = fsize - BUFFER_SIZE;
                           cached_buffers--;
                        } 

                        std::filesystem::resize_file(fname, newfsize);
                     }

                  } 
               fmtx.unlock();

            } // end if flush_allowed
            
         } else { // end if (cached_buffers > 0)
               
            flush_sleep_us = MAX_FLUSH_SLEEP_US;
            std::this_thread::sleep_for(std::chrono::microseconds(flush_sleep_us));
         }

      } else std::this_thread::sleep_for(std::chrono::milliseconds(50));

   }  // end while(true)

   delete[] cbuf;
};


void start_data_threads(DataThreads& threads){
   workers_running.store(true);

   if (mode == "remote"){
      threads.tx = std::thread(zmq_transfer, "zmq_xfer");
      threads.cache = std::thread(disk_cache, "disk_cache");
      threads.flush = std::thread(disk_flush, "disk_flush");
      
   }

   threads.dma = std::thread(dma_transfer, "dma_xfer");
}


void stop_data_threads(DataThreads& threads){
   acquisition_running.store(false);
   workers_running.store(false);

   q1.stop();
   q2.stop();
   q3.stop();

   if (threads.dma.joinable()){
      threads.dma.join();
   }

   if (threads.tx.joinable()){
      threads.tx.join();
   }

   if (threads.cache.joinable()){
      threads.cache.join();
   }

   if (threads.flush.joinable()){
      threads.flush.join();
   }
}


void prepare_buffer_queues(){
   q1.reset();
   q2.reset();
   q3.reset();

   for (uint32_t i = 0; i < RX_BUFFER_COUNT; i++){
      rx_channel.buf_ptr[i].status = PROXY_NO_ERROR;
      rx_channel.buf_ptr[i].length = 0;
      rx_channel.buf_ptr[i].transferred_length = 0;

      q1.push(static_cast<uint16_t>(i));
   }
}


bool reset_producer(DataThreads& threads){
   std::cout << "RESET: stopping data threads" << std::endl;

   stop_data_threads(threads);

   std::cout << "RESET: terminating DMA transfer" << std::endl;

   if (ioctl(rx_channel.fd, RESET_DMA) < 0){
      perror("RESET_DMA");
      std::cerr << "RESET FAILED: data threads remain stopped" << std::endl;

      return false;
   }

   if (mode == "remote"){
      std::ofstream empty_cache(fname, std::ios::binary | std::ios::out | std::ios::trunc);
      if (!empty_cache){
         std::cerr << "RESET FAILED: cannot truncate cache file" << std::endl;
         return false;
      }

      empty_cache.close();

      if (!empty_cache){
         std::cerr << "RESET FAILED: cannot close cache file" << std::endl;
         return false;
      }
   }

   completed_not_sent.store(0);
   dma_enabled.store(true);
   dma_idle.store(true);
   {
      std::lock_guard<std::mutex> lock(dma_state_mutex);
      dma_submitted = false;
      dma_failed = false;
   }
   cached_buffers.store(0);
   flush_allowed.store(false);
   flush_sleep_us.store(MAX_FLUSH_SLEEP_US);
   tx_flush_bandwith_mbps.store(0);

   prepare_buffer_queues();
   start_data_threads(threads);
   std::cout << "RESET COMPLETE: producer stopped, waiting for START" << std::endl;

   return true;
}

int main(int argc, const char **argv) {

   argparse::ArgumentParser program("evproducer");

   signal(SIGINT, intHandler);

   // START parsing command line options

   program.add_argument("--local")
    .help("local mode")
    .default_value(false)
    .implicit_value(true);

   program.add_argument("--debug")
    .help("enable debug")
    .default_value(false)
    .implicit_value(true);

   program.add_argument("--host")
    .help("receiver hostname");

   program.add_argument("--port")
    .help("receiver port")
    .default_value(uint16_t(5555))
    .scan<'u', uint16_t>();

   program.add_argument("--id")
    .help("MPMT id")
    .default_value(uint16_t(1))
    .scan<'u', uint16_t>();

   program.add_argument("--size")
    .help("DMA block size")
    .default_value(uint16_t(32768))
    .scan<'u', uint16_t>();

   program.add_argument("--disable-rc")
    .help("disable ZMQ run control")
    .default_value(false)
    .implicit_value(true);

   try {
      program.parse_args(argc, argv);
   } catch (const std::runtime_error& err) {
      std::cerr << err.what() << std::endl;
      std::cerr << program;
      return EXIT_FAILURE;
   }

   if(program.get<bool>("--local")) {
      mode = "local";
   } else if(program.present("--host")) {
      host = program.get<std::string>("--host");
      port = program.get<uint16_t>("--port");
      mpmtid = program.get<uint16_t>("--id");
      mode = "remote";
   } else {
      std::cerr << "E: specify local or host/id options" << std::endl;
      return EXIT_FAILURE;
   }

   size = program.get<uint16_t>("--size");
   if (size == 0 || size > BUFFER_SIZE){
      std::cerr << "E: invalid DMA block size" << std::endl;
      return EXIT_FAILURE;
   }
   debug = program.get<bool>("--debug");
   runcontrol = !(program.get<bool>("--disable-rc"));

   // END parsing command line options

   // open eth0 tx statistics
   std::ifstream eth0_stats_tx = std::ifstream(ETH0_TX_BYTES_FILE);

   // DMA

   std::string channel_name = "/dev/" + rx_channel_name;
   rx_channel.fd = open(channel_name.c_str(), O_RDWR);
   if (rx_channel.fd < 0) {
      std::cout << "E: unable to open DMA proxy device file: " << channel_name << std::endl;
      exit(EXIT_FAILURE);
   }

   rx_channel.buf_ptr = (struct channel_buffer *)mmap(NULL, sizeof(struct channel_buffer) * RX_BUFFER_COUNT,
      PROT_READ | PROT_WRITE, MAP_SHARED, rx_channel.fd, 0);
   if (rx_channel.buf_ptr == MAP_FAILED) {
      std::cout << "E: failed to mmap rx channel" << std::endl;
      exit(EXIT_FAILURE);
   }

   std::thread control_thread;
   DataThreads data_threads;

   if (mode == "remote"){
      sock = zmq::socket_t(ctx, zmq::socket_type::dealer);

      const std::string url = "tcp://" + host + ":" + std::to_string(port);
      sock.set(zmq::sockopt::routing_id, std::to_string(mpmtid));
      /* queue messages only to completed connections */
      sock.set(zmq::sockopt::immediate, 1);
      /* set linger period for socket shutdown - 0 no linger period */
      sock.set(zmq::sockopt::linger, 0);
      sock.connect(url);

      fname = std::string(EVENT_CACHE_FILE) + "." + std::to_string(std::time(0));
   }

   prepare_buffer_queues();
   acquisition_running.store(mode=="local" || !runcontrol);
   start_data_threads(data_threads);
   bool producer_ready = true;
   if (mode == "remote" && runcontrol){
      control_thread = std::thread(control, "control");
   }

   // std::thread th0, th1, th2, th3, th4;
   // if(mode == "remote") {

   //    sock = zmq::socket_t(ctx, zmq::socket_type::dealer);

   //    const std::string url = "tcp://" + host + ":" + std::to_string(port);
   //    sock.set(zmq::sockopt::routing_id, std::to_string(mpmtid));
   //    /* queue messages only to completed connections */
   //    sock.set(zmq::sockopt::immediate, 1);
   //    /* set linger period for socket shutdown - 0 no linger period */
   //    sock.set(zmq::sockopt::linger, 0);
   //    sock.connect(url);

   //    if(runcontrol)
   //       th0 = std::thread(control, "control");
   //    else run_state = "running";

   //    th2 = std::thread(zmq_transfer, "zmq_xfer");

   //    fname = std::string(EVENT_CACHE_FILE) + "." + std::to_string(std::time(0));

   //    th3 = std::thread(disk_cache, "disk_cache");
   //    th4 = std::thread(disk_flush, "disk_flush");
   // } else run_state = "running";

   // // before main loop preload DMA transfer queue
   // for(uint16_t i=0; i<RX_BUFFER_COUNT; i++)
   //    q1.push(i);

   // th1 = std::thread(dma_transfer, "dma_xfer");

   uint64_t prev_tx_bytes = 0;
   std::string line;

   if (std::getline(eth0_stats_tx, line)) {
      try {
         prev_tx_bytes = std::stoull(line);
      } catch (const std::exception&) {
         prev_tx_bytes = 0;
      }
   }
   // main loop


   auto next_statistics =
      std::chrono::steady_clock::now();

   while (!shutdown_requested) {
      PendingCommand request;
      if (take_command(request)) {
         bool success = true;
         std::string error;
         const bool confirmed = !request.request_id.empty();
         try {
            switch (request.command) {
            case ProducerCommand::START:
               if (!producer_ready || (confirmed && acquisition_running.load())) {
                  success = false;
                  error = "Producer not ready or already running";
               } else {
                  acquisition_running.store(true);
                  if (confirmed) {
                     std::unique_lock<std::mutex> lock(dma_state_mutex);
                     success = dma_state_changed.wait_for(lock, std::chrono::seconds(2),
                        [] { return dma_submitted || dma_failed; }) && dma_submitted && !dma_failed;
                     if (!success) {
                        acquisition_running.store(false);
                        producer_ready = false;
                        error = "DMA submission failed or timed out";
                     }
                  }
               }
               break;
            case ProducerCommand::STOP:
               if (!confirmed) {
                  acquisition_running.store(false); // original legacy pause
               } else {
                  dma_enabled.store(false);
                  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                  while ((!dma_idle.load() || completed_not_sent.load() != 0) &&
                         acquisition_running.load() && std::chrono::steady_clock::now() < deadline)
                     std::this_thread::sleep_for(std::chrono::milliseconds(5));
                  {
                     std::lock_guard<std::mutex> lock(dma_state_mutex);
                     success = dma_idle.load() && completed_not_sent.load() == 0 && !dma_failed;
                  }
                  stop_data_threads(data_threads);
                  producer_ready = false; // requires RESET before another START
                  if (!success) error = "DMA error or completed buffers not drained";
               }
               break;
            case ProducerCommand::RESET:
               producer_ready = false;
               producer_ready = reset_producer(data_threads);
               success = producer_ready;
               if (!success) error = "DMA reset or software cleanup failed";
               break;
            }
         } catch (const std::exception& exception) {
            success = false;
            producer_ready = false;
            acquisition_running.store(false);
            error = exception.what();
         }
         if (confirmed) {
            std::lock_guard<std::mutex> lock(reply_mutex);
            completed_commands.push({request.command, request.request_id, success, error});
         }
         if (!success) std::cerr << command_name(request.command) << ": " << error << std::endl;
      }


      const auto now =
         std::chrono::steady_clock::now();

      if (now >= next_statistics) {
         next_statistics =
               now + std::chrono::seconds(1);

         if (acquisition_running.load()) {
               eth0_stats_tx.clear();
               eth0_stats_tx.seekg(0);

               line.clear();

               uint64_t current_tx_bytes = 0;

               if (std::getline(eth0_stats_tx, line)) {
                  try {
                     current_tx_bytes = std::stoull(line);

                     if (current_tx_bytes >= prev_tx_bytes)
                           tx_bandwith =
                              current_tx_bytes - prev_tx_bytes;
                     else
                           tx_bandwith = 0;

                     prev_tx_bytes = current_tx_bytes;

                     tx_bandwith_mbps.store(
                           tx_bandwith * 8 / 1024 / 1024
                     );
                  } catch (const std::exception& error) {
                     std::cerr
                           << "TX statistics: "
                           << error.what()
                           << std::endl;
                  }
               }

               std::cout
                  << "Q1/Q2/Q3 size: "
                  << q1.size() << "/"
                  << q2.size() << "/"
                  << q3.size()
                  << " , buffers cached/tx.allowed: "
                  << cached_buffers.load() << "/"
                  << flush_allowed.load()
                  << " , eth0.tx (Mbit/s): "
                  << tx_bandwith_mbps.load()
                  << " , flush.tx (Mbit/s): "
                  << tx_flush_bandwith_mbps.load()
                  << " , flush.sleep (us): "
                  << flush_sleep_us.load()
                  << std::endl;
         }
      }

      std::this_thread::sleep_for(
         std::chrono::milliseconds(50)
      );
   }

   // network bandwith monitor
   // eth0_stats_tx.sync();
   // eth0_stats_tx.seekg(0);
   // line.clear();
   // std::getline(eth0_stats_tx, line);
   // prev_tx_bytes = std::stoll(line);

   // while(keep_running) {

   //    if(run_state == "running") {

   //       // network bandwith monitor
   //       eth0_stats_tx.clear();
   //       eth0_stats_tx.seekg(0);
   //       line.clear();
   //       std::getline(eth0_stats_tx, line);
   //       tx_bytes = std::stoll(line);

   //       if(tx_bytes >= prev_tx_bytes)
   //          tx_bandwith = tx_bytes - prev_tx_bytes;
   //       else
   //          tx_bandwith = std::numeric_limits<uint32_t>::max() - prev_tx_bytes + tx_bytes;

   //       tx_bandwith_mbps = (tx_bandwith * 8) / 1024 / 1024;
   //       prev_tx_bytes = tx_bytes;

   //       // some stats here...
   //       std::cout << "Q1/Q2/Q3 size: " << q1.size() << "/" << q2.size() << "/" << q3.size() << " , buffers cached/tx.allowed: " << cached_buffers.load() << "/" << flush_allowed << " , eth0.tx (Mbit/s): " << tx_bandwith_mbps << " , flush.tx (Mbit/s): " << tx_flush_bandwith_mbps << " , flush.sleep (us): " << flush_sleep_us << std::endl;

   //    }

   //    sleep(1);

   // } // end while

   // q1.stop();
   // q2.stop();
   // q3.stop();

   // if(mode == "remote") {
   //    if(runcontrol) {
   //       th0.join();
   //       printf(">>>> control thread joined <<<<\n");
   //    }

   //    th2.join();
   //    printf(">>>> zmq_xfer thread joined <<<<\n");
      
   //    th3.join();
   //    printf(">>>> disk_cache thread joined <<<<\n");

   //    th4.join();
   //    printf(">>>> disk_flush thread joined <<<<\n");
   // }

   // th1.join();
   // printf(">>>> dma_xfer thread joined <<<<\n");

   // eth0_stats_tx.close();

   // // DMA close
   // munmap(rx_channel.buf_ptr, sizeof(struct channel_buffer));
   // close(rx_channel.fd);

   program_running.store(false);

   if (control_thread.joinable())
      control_thread.join();

   
   stop_data_threads(data_threads);

   if (ioctl(rx_channel.fd, RESET_DMA) < 0)
      perror("RESET_DMA during shutdown");

   eth0_stats_tx.close();

   if (mode == "remote")
      sock.close();

   munmap(rx_channel.buf_ptr, sizeof(struct channel_buffer)* RX_BUFFER_COUNT);

   close(rx_channel.fd);

   std::cout << "Bye!" << std::endl;

   return EXIT_SUCCESS;
}
