# Twilio Outbound Call Setup Guide

This guide explains how to use the `twilio_integration_outbound.py` file to make outbound calls with Google ADK, STT, and TTS integration.

## Key Differences from Inbound Calls

- **Inbound**: Twilio receives a call → Twilio makes a webhook request to your server
- **Outbound**: Your server initiates the call → Twilio calls the destination → Twilio makes a webhook request to your server

## Prerequisites

1. **Twilio Account**: Sign up at [twilio.com](https://www.twilio.com)
2. **Twilio Phone Number**: Purchase a Twilio phone number
3. **Environment Variables**: Set up the following in your `.env` file or environment:

```env
TWILIO_ACCOUNT_SID=your_account_sid_here
TWILIO_AUTH_TOKEN=your_auth_token_here
TWILIO_PHONE_NUMBER=+1234567890  # Your Twilio phone number (E.164 format)
BASE_URL=https://your-domain.com  # Your public server URL (for webhooks)
```

4. **Public URL**: Your server must be publicly accessible (e.g., using ngrok, or deployed to a cloud service)

## Configuration

1. **Update the file** with your agent engine resource ID:
   ```python
   resource_id = "<your agent engine resource id>"
   ```

2. **Update Google credentials** (if not using environment variables):
   ```python
   os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "<your google application credentials file>"
   ```

## Usage

### 1. Start the Server

```bash
python twilio_integration_outbound.py
```

The server will start on `http://0.0.0.0:5050` by default.

### 2. Make an Outbound Call

Make a POST request to `/api/call` with the destination phone number:

```bash
curl -X POST http://localhost:5050/api/call \
  -H "Content-Type: application/json" \
  -d '{
    "to": "+1234567890"
  }'
```

**Response:**
```json
{
  "success": true,
  "call_sid": "CAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "status": "queued",
  "to": "+1234567890",
  "from": "+1234567890",
  "session_id": "session_id_here",
  "user_id": "user_id_here"
}
```

### 3. Call Flow

1. **Your server** receives POST request to `/api/call`
2. **Your server** creates an ADK session for the call
3. **Your server** calls Twilio REST API to initiate the call
4. **Twilio** calls the destination number
5. When the call is **answered**, Twilio makes a webhook request to `/incoming-call`
6. **Your server** returns TwiML instructing Twilio to open a media stream
7. **Twilio** connects to `/media-stream` WebSocket
8. **Conversation begins**: STT → Agent → TTS

## API Endpoints

### POST `/api/call`
Initiates an outbound call.

**Request Body:**
```json
{
  "to": "+1234567890"  // Destination phone number in E.164 format
}
```

**Response:**
```json
{
  "success": true,
  "call_sid": "CA...",
  "status": "queued",
  "to": "+1234567890",
  "from": "+1234567890",
  "session_id": "...",
  "user_id": "..."
}
```

### POST `/api/call-status` (Optional)
Webhook endpoint for Twilio call status updates. Configured automatically when making calls.

### POST `/incoming-call`
Handles webhook from Twilio when call is answered. Returns TwiML to start media stream.

### WebSocket `/media-stream`
Handles bidirectional audio stream with Twilio. Same as inbound calls.

### GET `/api/calls`
View all active calls in the registry.

### GET `/api/calls/{call_sid}`
View details of a specific call.

### DELETE `/api/calls/{call_sid}`
Remove a call from the registry.

## Example: Python Client

```python
import requests
import json

# Make an outbound call
response = requests.post(
    "http://localhost:5050/api/call",
    json={"to": "+1234567890"},
    headers={"Content-Type": "application/json"}
)

result = response.json()
print(f"Call initiated: {result['call_sid']}")
print(f"Status: {result['status']}")
```

## Example: JavaScript/Node.js Client

```javascript
const axios = require('axios');

async function makeOutboundCall(toNumber) {
  try {
    const response = await axios.post('http://localhost:5050/api/call', {
      to: toNumber
    });
    
    console.log('Call initiated:', response.data.call_sid);
    console.log('Status:', response.data.status);
    return response.data;
  } catch (error) {
    console.error('Error making call:', error.response?.data || error.message);
  }
}

// Usage
makeOutboundCall('+1234567890');
```

## Testing with ngrok (Local Development)

1. **Start ngrok** to expose your local server:
   ```bash
   ngrok http 5050
   ```

2. **Update BASE_URL** in your `.env` file or environment:
   ```env
   BASE_URL=https://your-ngrok-url.ngrok.io
   ```

3. **Restart your server** so it uses the new BASE_URL

4. **Make test calls** using the ngrok URL

## Features

- ✅ Full ADK integration with session management
- ✅ Real-time STT using Google Cloud Speech
- ✅ Agent responses using Google ADK
- ✅ Natural TTS using Google Cloud Text-to-Speech
- ✅ Interruption handling (user can interrupt bot)
- ✅ Call status tracking
- ✅ Automatic stream restart for long calls
- ✅ Session persistence across turns

## Troubleshooting

1. **"Twilio client not configured"**: Check your environment variables
2. **"Call not connecting"**: Verify your BASE_URL is publicly accessible
3. **"No session found"**: The call may not have been properly initiated
4. **WebSocket connection fails**: Ensure your server supports WSS (WebSocket Secure) for production

## Notes

- Phone numbers must be in E.164 format (e.g., `+1234567890`)
- Your server must be publicly accessible for Twilio to send webhooks
- For production, use HTTPS/WSS and proper authentication
- Consider rate limiting the `/api/call` endpoint to prevent abuse

