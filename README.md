# Tafil Signaling Server

WebSocket signaling server for Tafil P2P collaboration. Compatible with [y-webrtc](https://github.com/yjs/y-webrtc) signaling protocol.

## Deploy to Render (Free)

1. Push this repo to GitHub
2. Go to [render.com](https://render.com) → sign in with GitHub
3. Click **New → Web Service**
4. Connect this repo
5. Settings:
   - **Name:** `tafil-signaling`
   - **Runtime:** Node
   - **Build Command:** `npm install`
   - **Start Command:** `npm start`
   - **Instance Type:** Free
6. Click **Deploy**

Your signaling URL will be: `wss://tafil-signaling.onrender.com/ws/signaling`

## Health Check

```bash
curl https://tafil-signaling.onrender.com/health
```

## Local Development

```bash
npm install
npm start
# WebSocket: ws://localhost:4444/ws/signaling
# Health:    http://localhost:4444/health
```
