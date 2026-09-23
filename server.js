const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mediasoup = require('mediasoup');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

const SERVER_IP = '127.0.0.1'; 

let worker;
let router;
let producerVideoId; 
let producerAudioId;
let streamerSocketId = null;

const mediaCodecs = [
    { kind: 'audio', mimeType: 'audio/opus', clockRate: 48000, channels: 2 },
    { kind: 'video', mimeType: 'video/VP8', clockRate: 90000 }
  ];

const LOCAL_IP = '127.0.0.1';// Your IP Address;
const webRtcTransportOptions = {
  listenInfos: [
    { 
    protocol: 'udp', 
    ip: LOCAL_IP, 
    announcedAddress: LOCAL_IP 
    // KUNCI PERBAIKAN: Hapus properti port agar Mediasoup otomatis memilih port kosong antara 10000 - 10100
    }, 
    // { 
    // protocol: 'tcp', 
    // ip: LOCAL_IP, 
    // announcedAddress: LOCAL_IP, 
    // port: 443 // Jalur TCP 443 tetap statis sebagai pintu masuk penyamaran cadangan
    // }
  ],
  enableUdp: true,
  enableTcp: false,
  preferUdp: true
};

(async () => {
  worker = await mediasoup.createWorker({
    rtcMinPort: 10000,
    rtcMaxPort: 10100
  });
  console.log('ini dieksekusi...');
  
  router = await worker.createRouter({ mediaCodecs });
  console.log('Mediasoup Router Siap !');
})();


const rooms = new Map(); 
/* 
  Struktur isi Map 'rooms':
  roomName => {
     router: objectMediasoupRouter,
     streamerSocketId: string,
     producerVideoId: string,
     producerAudioId: string,
     subscribers: Set([socketId1, socketId2])
  }
*/


// PENYIMPANAN DATA KONEKSI
const socketTransports = new Map(); // id socket -> { sendTransport, recvTransport }
const socketConsumers = new Map();  // id socket -> [consumerVideo, consumerAudio]

io.on('connection', (socket) => {
  console.log(`User baru terhubung via WebSocket: ${socket.id}`);
  
  socket.on('joinRoom', ({ roomName }, callback) => {
      console.log(`User [${socket.id}] bergabung ke grup WebSocket Room: ${roomName}`);
      
      // 1. Masukkan socket ke dalam room internal Socket.io
      socket.join(roomName);
      
      // 2. Jika room sudah ada di memori Mediasoup dan ada streamer aktif di sana,
      // langsung beritahu penonton baru ini secara instan agar tombol tontonnya aktif
      const room = rooms.get(roomName);
      let activeStreamer = null;
      if (room) {
        // if (room.producerVideoId && room.producerAudioId) {
        //     socket.emit('newStreamerAvailable', {
        //         videoProducerId: room.producerVideoId,
        //         audioProducerId: room.producerAudioId
        //     });
        // }
        // Catat user sebagai subscriber di room tersebut
        room.subscribers.add(socket.id);
        if (room.producerVideoId && room.producerAudioId) {
          activeStreamer = {
              videoProducerId: room.producerVideoId,
              audioProducerId: room.producerAudioId
          };
        }
      }
      
      callback({ status: 'success', roomName, streamerData: activeStreamer });
  });

  // Inisialisasi kontainer transport untuk user baru
  socketTransports.set(socket.id, { sendTransport: null, recvTransport: null });

  // socket.on('getRouterRtpCapabilities', (callback) => {
  //   callback(router.rtpCapabilities);
  // });

  socket.on('getRouterRtpCapabilities', async ({ roomName }, callback) => {
    try {
        // Jika room belum ada di memori server, buat room baru beserta routernya
        if (!rooms.has(roomName)) {
            const router = await worker.createRouter({ mediaCodecs }); // Sesuai nama objek worker Anda
            rooms.set(roomName, {
                router: router,
                streamerSocketId: null,
                producerVideoId: null,
                producerAudioId: null,
                subscribers: new Set()
            });
            console.log(`[ROOM CREATED] Room baru berhasil dibuat: ${roomName}`);
        }

          const room = rooms.get(roomName);
          callback(room.router.rtpCapabilities);
      } catch (err) {
          console.error(err);
          callback({ error: err.message });
      }
  });

  socket.on('createWebRtcTransport', async ({ type, roomName }, callback) => {
    const room = rooms.get(roomName);
    if (!room) return callback({ error: 'Room tidak ditemukan' });

    try {
        // Gunakan router spesifik milik room tersebut, bukan router global
        const transport = await room.router.createWebRtcTransport(webRtcTransportOptions);

        // Simpan transport ke tracker socket lama Anda (tambahkan properti roomName)
        // if (!socketTransports.has(socket.id)) {
        //     socketTransports.set(socket.id, { roomName });
        // }

        // PERBAIKAN UTAMA: Selalu pastikan roomName diperbarui/dimasukkan ke dalam map pelacak
        const currentData = socketTransports.get(socket.id) || {};
        socketTransports.set(socket.id, {
            ...currentData,
            roomName: roomName // Mengunci nama kamar dengan aman
        });
        
        if (type === 'send') socketTransports.get(socket.id).sendTransport = transport;
        if (type === 'recv') socketTransports.get(socket.id).recvTransport = transport;

        callback({
            params: {
                id: transport.id,
                iceParameters: transport.iceParameters,
                iceCandidates: transport.iceCandidates,
                dtlsParameters: transport.dtlsParameters,
            }
        });
    } catch (err) {
        console.error(err);
        callback({ error: err.message });
    }
  });

  socket.on('connectTransport', async ({ type, dtlsParameters }, callback) => {
    const userTransports = socketTransports.get(socket.id);
    const transport = type === 'send' ? userTransports.sendTransport : userTransports.recvTransport;
    if (transport) {
      await transport.connect({ dtlsParameters });
    }
    callback();
  });

  socket.on('produce', async ({ kind, rtpParameters }, callback) => {
    const userTransportData = socketTransports.get(socket.id);
    if (!userTransportData) return callback({ error: 'Transport data tidak ditemukan' });

    const room = rooms.get(userTransportData.roomName);
    if (!room) return callback({ error: 'Room tidak ditemukan' });

    try {
        const transport = userTransportData.sendTransport;
        const producer = await transport.produce({ kind, rtpParameters });

        // Kunci ID streamer ke dalam objek room spesifik
        room.streamerSocketId = socket.id;
        if (kind === 'video') room.producerVideoId = producer.id;
        if (kind === 'audio') room.producerAudioId = producer.id;

        // Jika kedua track (video & audio) sudah siap, siarkan ke penonton yang sudah stand-by di room tersebut
        if (room.producerVideoId && room.producerAudioId) {
            console.log(`[STREAMING LIVE] Streamer aktif di Room: ${userTransportData.roomName}`);
            
            // Kirim sinyal hanya ke orang-orang yang ada di dalam room yang sama
            socket.to(userTransportData.roomName).emit('newStreamerAvailable', {
                videoProducerId: room.producerVideoId,
                audioProducerId: room.producerAudioId
            });
        }

        callback({ id: producer.id });
    } catch (err) {
        console.error(err);
        callback({ error: err.message });
    }
  });

  socket.on('consume', async ({ rtpCapabilities, producerId }, callback) => {
    try {
      const userTransports = socketTransports.get(socket.id);
      const transport = userTransports.recvTransport;
      if (!transport) return callback({ error: 'Recv Transport tidak ditemukan' });

      const caps = (rtpCapabilities && rtpCapabilities.codecs) ? rtpCapabilities : router.rtpCapabilities;

      const consumer = await transport.consume({
        producerId: producerId,
        rtpCapabilities: caps,
        paused: true 
      });

      // PERBAIKAN: Simpan ke dalam array agar tidak saling menimpa antara Audio dan Video
      if (!socketConsumers.has(socket.id)) {
        socketConsumers.set(socket.id, []);
      }
      socketConsumers.get(socket.id).push(consumer);

      callback({
        params: {
          id: consumer.id,
          producerId: consumer.producerId,
          kind: consumer.kind,
          rtpParameters: consumer.rtpParameters,
        }
      });
    } catch (error) {
      console.error("Gagal melakukan consume:", error);
      callback({ error: error.message });
    }
  });

  socket.on('resumeConsumer', async (callback) => {
    // PERBAIKAN: Resume seluruh consumer (Audio & Video) milik user tersebut
    const consumers = socketConsumers.get(socket.id);
    if (consumers && consumers.length > 0) {
      for (const consumer of consumers) {
        await consumer.resume();
      }
    }
    callback();
  });

  socket.on('disconnect', () => {
    console.log(`User terputus: ${socket.id}`);

    const userTransportData = socketTransports.get(socket.id);
    
    // Pembersihan transport standar klien yang keluar
    if (userTransportData) {
        if (userTransportData.sendTransport) userTransportData.sendTransport.close();
        if (userTransportData.recvTransport) userTransportData.recvTransport.close();
        
        const roomName = userTransportData.roomName;
        const room = rooms.get(roomName);

        if (room) {
            // JIKA YANG KELUAR ADALAH STREAMER DI ROOM INI
            if (socket.id === room.streamerSocketId) {
                console.log(`[STREAMER LEFT] Streamer di Room [${roomName}] keluar. Membubarkan penonton...`);

                // Beritahu hanya penonton di room ini saja
                io.to(roomName).emit('streamerLeft');

                // Sisir dan matikan semua consumer milik subscriber di room ini
                room.subscribers.forEach(subSocketId => {
                    const consumers = socketConsumers.get(subSocketId);
                    if (consumers) {
                        consumers.forEach(c => c.close());
                        socketConsumers.delete(subSocketId);
                    }
                });

                // Hapus total room ini dari memori server agar RAM bersih
                room.router.close(); // Menutup router otomatis menghancurkan semua transport & producer internalnya
                rooms.delete(roomName);
            } else {
                // JIKA YANG KELUAR ADALAH PENONTON BIASA
                room.subscribers.delete(socket.id);
                const consumers = socketConsumers.get(socket.id);
                if (consumers) {
                    consumers.forEach(c => c.close());
                    socketConsumers.delete(socket.id);
                }
            }
        }
        socketTransports.delete(socket.id);
    }
  });

});

server.listen(3000, () => console.log('Server run ---> http://localhost:3000'));
