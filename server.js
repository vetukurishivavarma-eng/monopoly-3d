const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Serve static files
app.use(express.static(path.join(__dirname)));

// Serve the main game page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'monopoly.html'));
});

// Generate a random room code
function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}

// Game rooms
const rooms = new Map();

// Player connections
const connections = new Map(); // ws -> { playerId, roomCode }

wss.on('connection', (ws) => {
  console.log('New WebSocket connection');
  let playerId = null;
  let currentRoom = null;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      handleMessage(ws, msg);
    } catch (e) {
      console.error('Failed to parse message:', e);
    }
  });

  ws.on('close', () => {
    handleDisconnect(ws);
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
    handleDisconnect(ws);
  });
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'create_room':
      handleCreateRoom(ws, msg);
      break;
    case 'join_room':
      handleJoinRoom(ws, msg);
      break;
    case 'leave_room':
      handleLeaveRoom(ws);
      break;
    case 'start_game':
      handleStartGame(ws, msg);
      break;
    case 'game_action':
      handleGameAction(ws, msg);
      break;
    case 'chat_message':
      handleChatMessage(ws, msg);
      break;
    case 'ping':
      ws.send(JSON.stringify({ type: 'pong' }));
      break;
    default:
      console.log('Unknown message type:', msg.type);
  }
}

function handleCreateRoom(ws, msg) {
  const playerId = msg.playerId || crypto.randomUUID();
  const playerName = msg.playerName || 'Player';
  const roomCode = generateRoomCode();

  const room = {
    code: roomCode,
    players: [],
    state: 'waiting', // waiting, playing, ended
    gameState: null,
    hostId: playerId,
    createdAt: Date.now()
  };

  const player = {
    id: playerId,
    name: playerName,
    token: msg.playerToken || '🚗',
    ws: ws,
    connected: true
  };

  room.players.push(player);
  rooms.set(roomCode, room);
  connections.set(ws, { playerId, roomCode });

  ws.send(JSON.stringify({
    type: 'room_created',
    roomCode,
    playerId,
    hostId: playerId,
    players: room.players.map(p => ({ id: p.id, name: p.name, token: p.token, connected: p.connected }))
  }));

  console.log(`Room ${roomCode} created by ${playerName}`);
}

function handleJoinRoom(ws, msg) {
  const roomCode = msg.roomCode.toUpperCase();
  const playerId = msg.playerId || crypto.randomUUID();
  const playerName = msg.playerName || 'Player';

  const room = rooms.get(roomCode);
  if (!room) {
    ws.send(JSON.stringify({ type: 'error', message: 'Room not found. Check the room code and try again.' }));
    return;
  }

  if (room.state !== 'waiting') {
    ws.send(JSON.stringify({ type: 'error', message: 'Game already in progress.' }));
    return;
  }

  if (room.players.length >= 4) {
    ws.send(JSON.stringify({ type: 'error', message: 'Room is full (max 4 players).' }));
    return;
  }

  const player = {
    id: playerId,
    name: playerName,
    token: msg.playerToken || '🚗',
    ws: ws,
    connected: true
  };

  room.players.push(player);
  connections.set(ws, { playerId, roomCode });

  // Notify the joining player
  ws.send(JSON.stringify({
    type: 'room_joined',
    roomCode,
    playerId,
    hostId: room.hostId,
    players: room.players.map(p => ({ id: p.id, name: p.name, token: p.token, connected: p.connected }))
  }));

  // Notify all other players
  broadcastToRoom(roomCode, {
    type: 'player_joined',
    player: { id: player.id, name: player.name, token: player.token, connected: true },
    players: room.players.map(p => ({ id: p.id, name: p.name, token: p.token, connected: p.connected }))
  }, ws);

  console.log(`${playerName} joined room ${roomCode}`);
}

function handleLeaveRoom(ws) {
  const conn = connections.get(ws);
  if (!conn) return;

  const { roomCode, playerId } = conn;
  const room = rooms.get(roomCode);
  if (!room) return;

  room.players = room.players.filter(p => p.id !== playerId);
  connections.delete(ws);

  if (room.players.length === 0) {
    rooms.delete(roomCode);
    console.log(`Room ${roomCode} deleted (empty)`);
    return;
  }

  // If the host left, assign new host
  if (room.hostId === playerId && room.players.length > 0) {
    room.hostId = room.players[0].id;
    broadcastToRoom(roomCode, {
      type: 'new_host',
      hostId: room.hostId
    });
  }

  broadcastToRoom(roomCode, {
    type: 'player_left',
    playerId,
    players: room.players.map(p => ({ id: p.id, name: p.name, connected: p.connected }))
  });

  console.log(`Player ${playerId} left room ${roomCode}`);
}

function handleStartGame(ws, msg) {
  const conn = connections.get(ws);
  if (!conn) return;

  const { roomCode, playerId } = conn;
  const room = rooms.get(roomCode);
  if (!room) return;

  if (room.hostId !== playerId) {
    ws.send(JSON.stringify({ type: 'error', message: 'Only the host can start the game.' }));
    return;
  }

  if (room.players.length < 2) {
    ws.send(JSON.stringify({ type: 'error', message: 'Need at least 2 players to start.' }));
    return;
  }

  room.state = 'playing';

  // Broadcast game start with initial game state
  broadcastToRoom(roomCode, {
    type: 'game_started',
    gameState: msg.gameState,
    playerOrder: room.players.map(p => p.id)
  });

  console.log(`Game started in room ${roomCode}`);
}

function handleGameAction(ws, msg) {
  const conn = connections.get(ws);
  if (!conn) return;

  const { roomCode, playerId } = conn;
  const room = rooms.get(roomCode);
  if (!room) return;

  // Broadcast the action to all players (including sender for confirmation)
  broadcastToRoom(roomCode, {
    type: 'game_action',
    playerId,
    action: msg.action,
    actionData: msg.actionData,
    gameState: msg.gameState
  });
}

function handleChatMessage(ws, msg) {
  const conn = connections.get(ws);
  if (!conn) return;

  const { roomCode, playerId } = conn;
  const room = rooms.get(roomCode);
  if (!room) return;

  const player = room.players.find(p => p.id === playerId);
  if (!player) return;

  broadcastToRoom(roomCode, {
    type: 'chat_message',
    playerId,
    playerName: player.name,
    message: msg.message,
    timestamp: Date.now()
  });
}

function handleDisconnect(ws) {
  const conn = connections.get(ws);
  if (!conn) return;

  const { roomCode, playerId } = conn;
  const room = rooms.get(roomCode);
  if (!room) return;

  // Mark player as disconnected
  const player = room.players.find(p => p.id === playerId);
  if (player) {
    player.connected = false;
    player.ws = null;
  }

  broadcastToRoom(roomCode, {
    type: 'player_disconnected',
    playerId,
    players: room.players.map(p => ({ id: p.id, name: p.name, connected: p.connected }))
  });

  connections.delete(ws);
  console.log(`Player ${playerId} disconnected from room ${roomCode}`);

  // If all players disconnected, clean up after a timeout
  const allDisconnected = room.players.every(p => !p.connected);
  if (allDisconnected) {
    setTimeout(() => {
      const r = rooms.get(roomCode);
      if (r && r.players.every(p => !p.connected)) {
        rooms.delete(roomCode);
        console.log(`Room ${roomCode} cleaned up (all disconnected)`);
      }
    }, 60000);
  }
}

function broadcastToRoom(roomCode, message, excludeWs = null) {
  const room = rooms.get(roomCode);
  if (!room) return;

  const data = JSON.stringify(message);
  for (const player of room.players) {
    if (player.ws && player.ws.readyState === WebSocket.OPEN) {
      if (excludeWs && player.ws === excludeWs) continue;
      player.ws.send(data);
    }
  }
}

function broadcastToRoomExcept(roomCode, message, excludePlayerId) {
  const room = rooms.get(roomCode);
  if (!room) return;

  const data = JSON.stringify(message);
  for (const player of room.players) {
    if (player.id === excludePlayerId) continue;
    if (player.ws && player.ws.readyState === WebSocket.OPEN) {
      player.ws.send(data);
    }
  }
}

// Periodic cleanup of stale rooms (older than 24 hours)
setInterval(() => {
  const now = Date.now();
  const maxAge = 24 * 60 * 60 * 1000;
  for (const [code, room] of rooms) {
    if (now - room.createdAt > maxAge) {
      rooms.delete(code);
      console.log(`Room ${code} cleaned up (expired)`);
    }
  }
}, 60 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Monopoly Online server running on port ${PORT}`);
});
