import os
import logging
import base64
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from google.cloud import speech, texttospeech
import asyncio
import uuid
import json
from datetime import datetime
from google.adk.runners import Runner
from dotenv import load_dotenv
from vertexai import agent_engines

from twilio.twiml.voice_response import VoiceResponse, Connect, Say, Stream

# --------------------------------------------------------------------------------------
# Basic configuration
# --------------------------------------------------------------------------------------
resource_id="<your agent engine resource id>"

import uvicorn

# Use local credentials file for Google
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "<your google application credentials file>"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
APP_NAME = "ADK Streaming example"

# Connect to remote agent application
remote_app = agent_engines.get(resource_id)

# Load environment variables
load_dotenv()

# --- FastAPI app ---
app = FastAPI()

# --- Google Cloud API clients ---
speech_client = speech.SpeechAsyncClient()
tts_client = texttospeech.TextToSpeechAsyncClient()

# --------------------------------------------------------------------------------------
# In-Memory Call and Session Management
# --------------------------------------------------------------------------------------
# Global dictionary to store call information
call_registry = {}

def create_call_session(call_sid: str, caller_number: str, called_number: str):
    """Create a new agent session for a specific call."""
    try:
        # Generate unique user_id for this call
        user_id = str(uuid.uuid4())
        
        # Create session with the agent engine
        remote_session = remote_app.create_session(
            user_id=user_id, 
            state={"user_authenticated": 0, "caller_number": caller_number}
        )
        session_id = remote_session['id']
        
        # Store call information in memory
        call_info = {
            "call_sid": call_sid,
            "caller_number": caller_number,
            "called_number": called_number,
            "user_id": user_id,
            "session_id": session_id,
            "created_at": datetime.now().isoformat(),
            "status": "active"
        }
        
        # Add to global registry
        call_registry[call_sid] = call_info
        
        logging.info(f"Created session for call {call_sid}: user_id={user_id}, session_id={session_id}")
        return user_id, session_id
        
    except Exception as e:
        logging.error(f"Error creating call session: {e}")
        return None, None

def get_call_session(call_sid: str):
    """Get session information for a specific call."""
    return call_registry.get(call_sid)

def update_call_status(call_sid: str, status: str):
    """Update call status in registry."""
    if call_sid in call_registry:
        call_registry[call_sid]["status"] = status
        call_registry[call_sid]["updated_at"] = datetime.now().isoformat()

def get_all_calls():
    """Get all calls from registry."""
    return call_registry

# --------------------------------------------------------------------------------------
# Audio/STT/TTS parameters
# Twilio Media Streams send 8kHz PCMU (mu-law). We match STT and TTS to that for low latency.
# --------------------------------------------------------------------------------------
STREAMING_CONFIG = speech.StreamingRecognitionConfig(
    config=speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.MULAW,
        sample_rate_hertz=8000,
        language_code="en-US",
        enable_automatic_punctuation=True,
    ),
    interim_results=True,
)

# Size of a single frame we send back to Twilio. 160 bytes ≈ 20 ms at 8k PCMU.
PCMU_FRAME_BYTES = 160

# --------------------------------------------------------------------------------------
# Basic health route
# --------------------------------------------------------------------------------------
@app.get("/", response_class=JSONResponse)
async def index_page():
    """Health check endpoint to verify server is up."""
    return {"message": "Twilio Media Stream Server is running!"}

# --------------------------------------------------------------------------------------
# Twilio webhook to start the media stream
# --------------------------------------------------------------------------------------
@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    """Return TwiML instructing Twilio to open a media stream to our WebSocket."""
    # Get caller information from Twilio webhook parameters
    form_data = await request.form()
    caller_number = form_data.get("From", "Unknown")
    called_number = form_data.get("To", "Unknown")
    call_sid = form_data.get("CallSid", "Unknown")
    
    logging.info(f"Incoming call from: {caller_number} to: {called_number} (CallSid: {call_sid})")
    
    # Create session for this specific call
    user_id, session_id = create_call_session(call_sid, caller_number, called_number)
    
    if not user_id or not session_id:
        logging.error(f"Failed to create session for call {call_sid}")
        response = VoiceResponse()
        response.say("Sorry, there was an error setting up your call. Please try again later.")
        return HTMLResponse(content=str(response), media_type="application/xml")
    
    response = VoiceResponse()
    response.say(
        "Please wait while we connect your call to the A. I. voice assistant, powered by Twilio and the Google S. T. T., Google A. D. K. and Google T. T. S. APIs",
        voice="Google.en-US-Chirp3-HD-Aoede"
    )
    # Intentionally avoid extra pause for lower startup latency
    response.say(
        "O.K. you can start talking!",
        voice="Google.en-US-Chirp3-HD-Aoede"
    )
    host = request.url.hostname
    connect = Connect()
    # Pass call_sid as query parameter to WebSocket
    connect.stream(url=f'wss://{host}/media-stream?call_sid={call_sid}')
    response.append(connect)
    return HTMLResponse(content=str(response), media_type="application/xml")

# --------------------------------------------------------------------------------------
# TTS helper
# --------------------------------------------------------------------------------------
async def synthesize_speech_for_response(text: str) -> str:
    """Synthesize text to 8kHz PCMU and return it as base64 string."""
    logging.info(f"Synthesizing speech for: {text}")
    synthesis_input = texttospeech.SynthesisInput(text=text)
    voice = texttospeech.VoiceSelectionParams(
        language_code="en-US", ssml_gender=texttospeech.SsmlVoiceGender.FEMALE
    )
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.MULAW,
        sample_rate_hertz=8000,
    )

    response = await tts_client.synthesize_speech(
        input=synthesis_input, voice=voice, audio_config=audio_config
    )
    return base64.b64encode(response.audio_content).decode('utf-8')

async def send_text_as_pcmu_frames(websocket: WebSocket, stream_sid: str, text: str) -> bool:
    """Synthesize the given text and stream it back to Twilio as PCMU frames."""
    if not stream_sid or websocket.client_state.name != 'CONNECTED':
        return False
    try:
        bot_audio_b64_full = await synthesize_speech_for_response(text)
        bot_audio_bytes = base64.b64decode(bot_audio_b64_full)
        sent = False
        for i in range(0, len(bot_audio_bytes), PCMU_FRAME_BYTES):
            if websocket.client_state.name != 'CONNECTED':
                break
            frame = bot_audio_bytes[i:i+PCMU_FRAME_BYTES]
            audio_delta = {
                "event": "media",
                "streamSid": stream_sid,
                "media": {
                    "payload": base64.b64encode(frame).decode('utf-8')
                }
            }
            await websocket.send_json(audio_delta)
            sent = True
        return sent
    except Exception as e:
        logging.error(f"Error sending TTS frames: {e}")
        return False

async def send_delayed_filler(websocket: WebSocket, stream_sid: str, delay_seconds: float, text: str):
    """After a delay, speak a short filler if not cancelled (for slow agent responses)."""
    try:
        await asyncio.sleep(delay_seconds)
        if stream_sid and websocket.client_state.name == 'CONNECTED':
            await send_text_as_pcmu_frames(websocket, stream_sid, text)
    except asyncio.CancelledError:
        # Normal path when agent responds before timeout
        return
    except Exception as e:
        logging.error(f"Error in delayed filler: {e}")

# --------------------------------------------------------------------------------------
# Optional API for testing TTS independently
# --------------------------------------------------------------------------------------
@app.post("/api/tts")
async def text_to_speech(request: Request):
    """Convert posted text to base64 audio using the same TTS settings used for calls."""
    data = await request.json()
    text_to_synthesize = data.get("text", "")
    audio_base64 = await synthesize_speech_for_response(text_to_synthesize)
    return {"audio_content": audio_base64}

# --------------------------------------------------------------------------------------
# WebSocket endpoint used by Twilio Media Streams
# --------------------------------------------------------------------------------------
@app.websocket("/media-stream")
async def websocket_stt_endpoint(websocket: WebSocket):
    """Bidirectional audio+text processing for the voice conversation with Twilio."""
    # Twilio requires the audio.twilio.com subprotocol
    await websocket.accept(subprotocol="audio.twilio.com")
    
    # Get call_sid from query parameters
    call_sid = websocket.query_params.get("call_sid", "unknown")
    logging.info(f"WebSocket STT connection accepted for call: {call_sid}")
    
    # Get session information for this call
    call_info = get_call_session(call_sid)
    if not call_info:
        logging.error(f"No session found for call {call_sid}")
        await websocket.close()
        return
    
    user_id = call_info["user_id"]
    session_id = call_info["session_id"]
    caller_number = call_info["caller_number"]
    
    logging.info(f"Using session - user_id: {user_id}, session_id: {session_id}, caller: {caller_number}")

    audio_queue = asyncio.Queue()
    stream_sid = None
    latest_media_timestamp = 0
    mark_queue = []
    response_start_timestamp_twilio = None

    async def receive_from_twilio():
        """Receive media and control events from Twilio and enqueue audio for STT."""
        nonlocal stream_sid, latest_media_timestamp
        try:
            async for message in websocket.iter_text():
                data = json.loads(message)
                event_type = data.get('event')
                if event_type == 'media':
                    latest_media_timestamp = int(data['media']['timestamp'])
                    audio_chunk_b64 = data['media']['payload']
                    # Push base64-encoded audio frames to the queue; decode in the generator
                    await audio_queue.put(audio_chunk_b64)
                elif event_type == 'start':
                    stream_sid = data['start']['streamSid']
                    logging.info(f"Incoming stream has started {stream_sid} for call {call_sid}")
                    # Reset per-stream state
                    response_start_timestamp_twilio = None
                    latest_media_timestamp = 0
                    last_assistant_item = None
                    # Update call status to streaming
                    update_call_status(call_sid, "streaming")
                elif event_type == 'mark':
                    if mark_queue:
                        mark_queue.pop(0)
        except WebSocketDisconnect:
            logging.info(f"Twilio disconnected the WebSocket for call {call_sid}")
            update_call_status(call_sid, "disconnected")
            await audio_queue.put(None)
        except RuntimeError as e:
            logging.error(f"WebSocket runtime error for call {call_sid}: {e}")
            update_call_status(call_sid, "error")
            await audio_queue.put(None)

    async def run_agent_and_send_response():
        """Stream audio to Google STT, send transcript to agent, TTS reply back to Twilio.
        Continues handling multiple user turns until Twilio closes the WebSocket.
        """
        nonlocal stream_sid, response_start_timestamp_twilio
        # Track last processed final transcript to avoid duplicate agent calls
        last_final_transcript = ""
        last_final_media_ts_ms = -1

        async def audio_generator():
            """Async generator feeding Google STT with initial config then audio frames."""
            # Send config first
            yield speech.StreamingRecognizeRequest(streaming_config=STREAMING_CONFIG)
            while True:
                chunk_b64 = await audio_queue.get()
                if chunk_b64 is None:
                    break
                # Twilio sends base64 PCMU frames; decode to raw bytes for Google STT
                yield speech.StreamingRecognizeRequest(audio_content=base64.b64decode(chunk_b64))

        try:
            # Stream recognition responses
            responses = await speech_client.streaming_recognize(requests=audio_generator())

            async for response in responses:
                if not response.results:
                    continue
                result = response.results[0]
                if not result.alternatives:
                    continue

                transcript = (result.alternatives[0].transcript or "").strip()

                if result.is_final:
                    # Ignore empty final transcripts
                    if not transcript:
                        logging.info("Final transcript was empty; skipping agent call.")
                        continue
                    # Debounce identical finals within 1.2s (likely STT duplicate); allow later repeats
                    current_media_ts_ms = latest_media_timestamp or 0
                    duplicate_within_window = (
                        transcript == last_final_transcript and
                        last_final_media_ts_ms >= 0 and
                        (current_media_ts_ms - last_final_media_ts_ms) < 1200
                    )
                    if duplicate_within_window:
                        logging.info("Duplicate final transcript within debounce window; skipping agent call.")
                        continue
                    # Record this final as processed
                    last_final_transcript = transcript
                    last_final_media_ts_ms = current_media_ts_ms

                    logging.info(f"Final transcript received: {transcript}")

                    # Send user's utterance to your agent and stream back the TTS response
                    user_final_text = transcript

                    # Kick off a delayed filler in case the agent response is slow
                    filler_task = asyncio.create_task(
                        send_delayed_filler(
                            websocket,
                            stream_sid,
                            delay_seconds=0.5,  # 500ms = 0.5 seconds
                            text="Just a moment while I process your request."
                        )
                    )

                    response_sent = False  # Flag to prevent duplicate responses
                    filler_cancelled = False  # Ensure we cancel filler only once

                    for event in remote_app.stream_query(
                            user_id=user_id,  # Use call-specific user_id
                            session_id=session_id,  # Use call-specific session_id
                            message=user_final_text,
                    ):
                        try:
                            # Extract text from agent event payload with better error handling
                            if 'content' in event and 'parts' in event['content'] and len(event['content']['parts']) > 0:
                                if 'text' in event['content']['parts'][0]:
                                    res = event['content']['parts'][0]['text']
                                    logging.info(f"Generated bot response for call {call_sid}: {res}")

                                    # Now that we have actual assistant text, cancel filler if pending (only once)
                                    if not filler_cancelled and not filler_task.done():
                                        filler_task.cancel()
                                        try:
                                            await filler_task
                                        except asyncio.CancelledError:
                                            pass
                                        filler_cancelled = True

                                    # Check if WebSocket is still connected before sending
                                    if websocket.client_state.name == 'CONNECTED':
                                        # Stream TTS back to Twilio as small frames
                                        bot_sent = await send_text_as_pcmu_frames(websocket, stream_sid, res)
                                        if bot_sent and not response_sent:
                                            response_sent = True
                                            # Reset duplicate guard so the same user word later is treated as new
                                            last_final_transcript = ""
                                            last_final_media_ts_ms = -1

                                else:
                                    logging.warning("No 'text' field found in agent response parts")
                            else:
                                logging.warning("Unexpected agent response structure")

                        except Exception as e:
                            # Cancel filler if pending and send fallback response only once
                            if not filler_cancelled and not filler_task.done():
                                filler_task.cancel()
                                try:
                                    await filler_task
                                except asyncio.CancelledError:
                                    pass
                            filler_cancelled = True
                            
                            if not response_sent and websocket.client_state.name == 'CONNECTED':
                                logging.error(f"Agent response error for call {call_sid}: {e}")
                                await send_text_as_pcmu_frames(websocket, stream_sid, "Sure, give me a moment")
                                response_sent = True
                else:
                    # Could surface interim transcripts if needed
                    pass
        except Exception as e:
            logging.error(f"Error during Google STT processing for call {call_sid}: {e}")
        finally:
            # Do not close the WebSocket; Twilio controls stream lifecycle
            logging.info("STT processing finished for this turn.")

    # Run receiver and agent/STT tasks concurrently
    try:
        await asyncio.gather(receive_from_twilio(), run_agent_and_send_response())
    finally:
        # Update call status when WebSocket closes
        update_call_status(call_sid, "ended")
        logging.info(f"Call {call_sid} session ended")

# Add API endpoints to view call registry
@app.get("/api/calls")
async def get_call_registry():
    """Get current call registry."""
    return {"calls": call_registry, "total_calls": len(call_registry)}

@app.get("/api/calls/{call_sid}")
async def get_call_info(call_sid: str):
    """Get information for a specific call."""
    call_info = get_call_session(call_sid)
    if call_info:
        return call_info
    else:
        return {"error": "Call not found"}

@app.delete("/api/calls/{call_sid}")
async def clear_call(call_sid: str):
    """Remove a call from registry."""
    if call_sid in call_registry:
        del call_registry[call_sid]
        return {"message": f"Call {call_sid} removed from registry"}
    else:
        return {"error": "Call not found"}

@app.delete("/api/calls")
async def clear_all_calls():
    """Clear all calls from registry."""
    global call_registry
    call_registry.clear()
    return {"message": "All calls cleared from registry"}

if __name__=="__main__":
    # export GOOGLE_APPLICATION_CREDENTIALS="./testvertexbot-1a0b45623d70.json"
    uvicorn.run("twilio_integration:app", host="0.0.0.0", port=5050, loop="uvloop", http="httptools", ws="websockets")

# --------------------------------------------------------------------------------------
# Startup warm-up: pre-initialize TTS to reduce first-response latency
# --------------------------------------------------------------------------------------
@app.on_event("startup")
async def warm_up_tts():
    try:
        _ = await synthesize_speech_for_response(".")
        logging.info("TTS warm-up completed")
    except Exception as e:
        logging.warning(f"TTS warm-up failed: {e}")