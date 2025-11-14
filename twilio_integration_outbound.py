import os
import logging
import traceback
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
from twilio.rest import Client as TwilioClient

# --------------------------------------------------------------------------------------
# Basic configuration
# --------------------------------------------------------------------------------------
# Load environment variables
load_dotenv()
resource_id = "<your agent engine resource id>"
import uvicorn

# Use local credentials file for Google
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "<your google application credentials file>"

# Twilio credentials - should be set in .env file
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")  # Your Twilio phone number
BASE_URL = os.getenv("BASE_URL", "https://your-domain.com")  # Your public server URL

# Initialize Twilio client
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
else:
    logging.warning("Twilio credentials not found. Outbound calls will not work.")
    twilio_client = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
APP_NAME = "ADK Outbound Streaming example"

# Connect to remote agent application
remote_app = agent_engines.get(resource_id)

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

def create_call_session(call_sid: str, caller_number: str, called_number: str, direction: str = "outbound"):
    """Create a new agent session for a specific call."""
    try:
        # Generate unique user_id for this call
        user_id = str(uuid.uuid4())
        
        # Create session with the agent engine
        remote_session = remote_app.create_session(
            user_id=user_id, 
            state={"user_authenticated": 0, "caller_number": caller_number, "called_number": called_number, "direction": direction}
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
            "status": "initiated" if direction == "outbound" else "active",
            "direction": direction
        }
        
        # Add to global registry
        call_registry[call_sid] = call_info
        
        logging.info(f"Created session for call {call_sid}: user_id={user_id}, session_id={session_id}, direction={direction}")
        return user_id, session_id, call_info
        
    except Exception as e:
        logging.error(f"Error creating call session: {e}")
        return None, None, None

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
    return {"message": "Twilio Outbound Media Stream Server is running!"}

# --------------------------------------------------------------------------------------
# API endpoint to initiate outbound calls
# --------------------------------------------------------------------------------------
@app.post("/api/call")
async def initiate_outbound_call(request: Request):
    """Initiate an outbound call to a destination phone number."""
    if not twilio_client:
        return JSONResponse(
            status_code=500,
            content={"error": "Twilio client not configured. Please set TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN in environment variables."}
        )
    
    try:
        data = await request.json()
        to_number = data.get("to")  # Destination phone number (E.164 format: +1234567890)
        
        if not to_number:
            return JSONResponse(
                status_code=400,
                content={"error": "Missing 'to' parameter. Please provide destination phone number in E.164 format."}
            )
        
        # Generate a call SID placeholder (will be replaced by actual CallSid from Twilio)
        temp_call_sid = f"temp_{uuid.uuid4().hex[:8]}"
        
        # Create session before making the call
        user_id, session_id, call_info = create_call_session(
            call_sid=temp_call_sid,
            caller_number=TWILIO_PHONE_NUMBER,
            called_number=to_number,
            direction="outbound"
        )
        
        if not user_id or not session_id:
            return JSONResponse(
                status_code=500,
                content={"error": "Failed to create session for outbound call"}
            )
        
        # Construct the webhook URL for when the call is answered
        webhook_url = f"{BASE_URL}/incoming-call"
        
        # Make the outbound call using Twilio REST API
        call = twilio_client.calls.create(
            to=to_number,
            from_=TWILIO_PHONE_NUMBER,
            url=webhook_url,  # TwiML instructions when call is answered
            method="POST",
            status_callback=f"{BASE_URL}/api/call-status",  # Optional: status webhook
            status_callback_event=["initiated", "ringing", "answered", "completed"],
            status_callback_method="POST"
        )
        
        # Update call registry with actual CallSid from Twilio
        actual_call_sid = call.sid
        if temp_call_sid in call_registry:
            call_info = call_registry.pop(temp_call_sid)
            call_info["call_sid"] = actual_call_sid
            call_info["twilio_call_sid"] = actual_call_sid
            call_registry[actual_call_sid] = call_info
        
        logging.info(f"Outbound call initiated: CallSid={actual_call_sid}, To={to_number}, From={TWILIO_PHONE_NUMBER}")
        
        return JSONResponse(content={
            "success": True,
            "call_sid": actual_call_sid,
            "status": call.status,
            "to": to_number,
            "from": TWILIO_PHONE_NUMBER,
            "session_id": session_id,
            "user_id": user_id
        })
        
    except Exception as e:
        logging.error(f"Error initiating outbound call: {e}")
        logging.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to initiate call: {str(e)}"}
        )

# --------------------------------------------------------------------------------------
# Twilio status callback webhook (optional)
# --------------------------------------------------------------------------------------
@app.post("/api/call-status")
async def handle_call_status(request: Request):
    """Handle Twilio call status callbacks."""
    try:
        form_data = await request.form()
        call_sid = form_data.get("CallSid")
        call_status = form_data.get("CallStatus")
        
        logging.info(f"Call status update: CallSid={call_sid}, Status={call_status}")
        
        # Update call registry with status
        if call_sid and call_sid in call_registry:
            update_call_status(call_sid, call_status.lower())
        
        return JSONResponse(content={"status": "ok"})
    except Exception as e:
        logging.error(f"Error handling call status: {e}")
        return JSONResponse(content={"status": "error", "error": str(e)})

# --------------------------------------------------------------------------------------
# Twilio webhook to start the media stream (same as inbound, but works for outbound too)
# --------------------------------------------------------------------------------------
@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    """Return TwiML instructing Twilio to open a media stream to our WebSocket.
    This endpoint is called when:
    - An outbound call is answered (we initiated the call)
    - An inbound call is received (Twilio receives a call to your number)
    """
    # Get caller information from Twilio webhook parameters
    form_data = await request.form()
    caller_number = form_data.get("From", "Unknown")
    called_number = form_data.get("To", "Unknown")
    call_sid = form_data.get("CallSid", "Unknown")
    
    # Determine call direction
    # For outbound calls: From = our Twilio number, To = destination
    # For inbound calls: From = caller, To = our Twilio number
    direction = "outbound" if caller_number == TWILIO_PHONE_NUMBER else "inbound"
    
    logging.info(f"Call received - Direction: {direction}, From: {caller_number}, To: {called_number}, CallSid: {call_sid}")
    
    # Get or create session for this call
    call_info = get_call_session(call_sid)
    
    if not call_info:
        # Create session if it doesn't exist (might happen if status callback hasn't fired yet)
        logging.warning(f"Session not found for call {call_sid}, creating new session")
        user_id, session_id, call_info = create_call_session(call_sid, caller_number, called_number, direction)
        
        if not user_id or not session_id:
            logging.error(f"Failed to create session for call {call_sid}")
            response = VoiceResponse()
            response.say("Sorry, there was an error setting up your call. Please try again later.")
            return HTMLResponse(content=str(response), media_type="application/xml")
    else:
        # Update status to active when call is answered
        update_call_status(call_sid, "active")
        user_id = call_info["user_id"]
        session_id = call_info["session_id"]
    
    logging.info(f"Using session - user_id: {user_id}, session_id: {session_id}")
    initial_message = "O.K. you can start talking!"
    
    response = VoiceResponse()
    response.say(
        "Please wait while we connect your call to the A. I. voice assistant, powered by the Google S. T. T., Google A. D. K. and Google T. T. S. APIs",
        voice="Google.en-US-Chirp3-HD-Aoede"
    )
    # Intentionally avoid extra pause for lower startup latency
    response.say(
        initial_message,
        voice="Google.en-US-Chirp3-HD-Aoede"
    )
    host = request.url.hostname
    connect = Connect()
    # Pass call_sid as custom parameter to WebSocket
    stream = connect.stream(url=f'wss://{host}/media-stream')
    stream.parameter(name='call_sid', value=call_sid)
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

# Add this function to send clear command to Twilio
async def clear_twilio_audio(websocket: WebSocket, stream_sid: str):
    """Send clear command to Twilio to stop current audio playback."""
    if stream_sid and websocket.client_state.name == 'CONNECTED':
        try:
            clear_command = {
                "event": "clear",
                "streamSid": stream_sid
            }
            await websocket.send_json(clear_command)
            logging.info("Sent clear command to Twilio")
        except Exception as e:
            logging.error(f"Error sending clear command: {e}")
            logging.error(traceback.format_exc())

async def stream_tts_with_interruption(websocket: WebSocket, stream_sid: str, text: str, interrupt_flag: asyncio.Event) -> bool:
    """Stream TTS audio with immediate interruption capability."""
    if not stream_sid or websocket.client_state.name != 'CONNECTED':
        return False
    
    try:
        # Synthesize the full audio first
        bot_audio_b64_full = await synthesize_speech_for_response(text)
        bot_audio_bytes = base64.b64decode(bot_audio_b64_full)
        
        sent = False
        # Stream in very small chunks for immediate interruption
        chunk_size = 80  # ~10ms chunks for faster interruption
        for i in range(0, len(bot_audio_bytes), chunk_size):
            # Check for interruption before each chunk
            if interrupt_flag.is_set():
                logging.info("TTS interrupted by user speech - sending clear command")
                # Send clear command to stop audio playback
                await clear_twilio_audio(websocket, stream_sid)
                break
            if websocket.client_state.name != 'CONNECTED':
                break
                
            frame = bot_audio_bytes[i:i+chunk_size]
            audio_delta = {
                "event": "media",
                "streamSid": stream_sid,
                "media": {
                    "payload": base64.b64encode(frame).decode('utf-8')
                }
            }
            await websocket.send_json(audio_delta)
            sent = True
            
            # Very small delay to allow frequent interruption checks
            await asyncio.sleep(0.005)  # 5ms delay between chunks
            
        return sent
    except Exception as e:
        logging.error(f"Error sending TTS frames: {e}")
        logging.error(traceback.format_exc())
        return False

async def send_delayed_filler(websocket: WebSocket, stream_sid: str, delay_seconds: float, text: str, interrupt_flag: asyncio.Event):
    """After a delay, speak a short filler if not cancelled (for slow agent responses)."""
    try:
        await asyncio.sleep(delay_seconds)
        if stream_sid and websocket.client_state.name == 'CONNECTED' and not interrupt_flag.is_set():
            await stream_tts_with_interruption(websocket, stream_sid, text, interrupt_flag)
    except asyncio.CancelledError:
        # Normal path when agent responds before timeout
        return
    except Exception as e:
        logging.error(f"Error in delayed filler: {e}")
        logging.error(traceback.format_exc())

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
# WebSocket endpoint used by Twilio Media Streams (same as inbound)
# --------------------------------------------------------------------------------------
@app.websocket("/media-stream")
async def websocket_stt_endpoint(websocket: WebSocket):
    """Bidirectional audio+text processing for the voice conversation with Twilio."""
    # Twilio requires the audio.twilio.com subprotocol
    await websocket.accept(subprotocol="audio.twilio.com")
    
    logging.info(f"WebSocket STT connection accepted, waiting for start event with call_sid...")

    audio_queue = asyncio.Queue()
    stream_sid = None
    latest_media_timestamp = 0
    mark_queue = []
    response_start_timestamp_twilio = None
    
    # These will be set after receiving the start event with call_sid
    call_sid = None
    user_id = None
    session_id = None
    caller_number = None
    call_info = None
    
    # Interruption handling based on Google STT interim results
    current_tts_task = None
    current_agent_task = None
    interrupt_flag = asyncio.Event()
    last_interim_time = 0
    interim_silence_threshold = 1.0  # seconds of silence before considering speech ended

    async def receive_from_twilio():
        """Receive media and control events from Twilio and enqueue audio for STT."""
        nonlocal stream_sid, latest_media_timestamp, call_sid, user_id, session_id, caller_number, call_info
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
                    # Get call_sid from custom parameters
                    custom_params = data['start'].get('customParameters', {})
                    call_sid = custom_params.get('call_sid', 'unknown')
                    
                    logging.info(f"Incoming stream has started {stream_sid} for call {call_sid}")
                    
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
                    
                    # Reset per-stream state
                    response_start_timestamp_twilio = None
                    latest_media_timestamp = 0
                    last_assistant_item = None
                    interrupt_flag.clear()
                    last_interim_time = 0
                    # Update call status to streaming
                    update_call_status(call_sid, "streaming")
                elif event_type == 'mark':
                    if mark_queue:
                        mark_queue.pop(0)
        except WebSocketDisconnect:
            logging.info(f"Twilio disconnected the WebSocket for call {call_sid if call_sid else 'unknown'}")
            if call_sid:
                update_call_status(call_sid, "disconnected")
            await audio_queue.put(None)
        except RuntimeError as e:
            logging.error(f"WebSocket runtime error for call {call_sid if call_sid else 'unknown'}: {e}")
            logger.error(traceback.format_exc())
            if call_sid:
                update_call_status(call_sid, "error")
            await audio_queue.put(None)

    async def process_agent_response(user_final_text: str, websocket: WebSocket, stream_sid: str, interrupt_flag: asyncio.Event):
        """Process agent response with interruption support."""
        nonlocal current_tts_task, current_agent_task
        
        try:
            # Clear any previous interruption flag
            interrupt_flag.clear()

            # Kick off a delayed filler in case the agent response is slow
            filler_task = asyncio.create_task(
                send_delayed_filler(
                    websocket,
                    stream_sid,
                    delay_seconds=0.5,  # 500ms = 0.5 seconds
                    text="Just a moment while I process your request.",
                    interrupt_flag=interrupt_flag
                )
            )

            response_sent = False  # Flag to prevent duplicate responses
            filler_cancelled = False  # Ensure we cancel filler only once

            for event in remote_app.stream_query(
                    user_id=user_id,
                    session_id=session_id,
                    message=user_final_text,
            ):
                # Check for interruption before processing each agent event
                if interrupt_flag.is_set():
                    logging.info("Agent processing interrupted by user speech")
                    break
                    
                try:
                    # Extract text from agent event payload with better error handling
                    if 'content' in event and 'parts' in event['content'] and len(event['content']['parts']) > 0:
                        if 'text' in event['content']['parts'][0]:
                            res = event['content']['parts'][0]['text']
                            logging.info(f"Generated bot response: {res}")

                            # Now that we have actual assistant text, cancel filler if pending (only once)
                            if not filler_cancelled and not filler_task.done():
                                filler_task.cancel()
                                try:
                                    await filler_task
                                except asyncio.CancelledError:
                                    pass
                                filler_cancelled = True

                            # Check if WebSocket is still connected and user is not speaking
                            if websocket.client_state.name == 'CONNECTED' and not interrupt_flag.is_set():
                                # Stream TTS back to Twilio with immediate interruption capability
                                current_tts_task = asyncio.create_task(
                                    stream_tts_with_interruption(websocket, stream_sid, res, interrupt_flag)
                                )
                                bot_sent = await current_tts_task
                                if bot_sent and not response_sent:
                                    response_sent = True

                        else:
                            logging.info("No 'text' field found in agent response parts")
                    else:
                        logging.info("Unexpected agent response structure")

                except Exception as e:
                    logging.error(f"Error processing agent response: {e}")
                    logging.error(traceback.format_exc())
                    # Cancel filler if pending and send fallback response only once
                    if not filler_cancelled and not filler_task.done():
                        filler_task.cancel()
                        try:
                            await filler_task
                        except asyncio.CancelledError:
                            pass
                    filler_cancelled = True
                    
                    if not response_sent and websocket.client_state.name == 'CONNECTED' and not interrupt_flag.is_set():
                        logging.error(f"Agent response error: {e}")
                        current_tts_task = asyncio.create_task(
                            stream_tts_with_interruption(websocket, stream_sid, "Sure, give me a moment", interrupt_flag)
                        )
                        await current_tts_task
                        response_sent = True
                        
        except asyncio.CancelledError:
            logging.info("Agent processing was cancelled due to user interruption")
            # Cancel any pending filler
            if 'filler_task' in locals() and not filler_task.done():
                filler_task.cancel()
        except Exception as e:
            logging.error(f"Error in agent processing: {e}")
            logging.error(traceback.format_exc())

    async def run_agent_and_send_response():
        """Stream audio to Google STT, send transcript to agent, TTS reply back to Twilio.
        Continues handling multiple user turns until Twilio closes the WebSocket.
        Handles automatic stream restart to avoid Google's 305-second limit.
        """
        nonlocal stream_sid, response_start_timestamp_twilio, current_tts_task, current_agent_task, interrupt_flag, last_interim_time
        # Track last processed final transcript to avoid duplicate agent calls
        last_final_transcript = ""
        last_final_media_ts_ms = -1
        
        # Maximum stream duration: restart before hitting Google's 305s limit
        MAX_STREAM_DURATION = 290  # seconds (leave 15s buffer before 305s limit)
        MAX_RESTARTS = 50  # Maximum number of stream restarts (allows ~4 hours of call time)
        
        restart_count = 0
        
        while restart_count < MAX_RESTARTS:  # Loop to handle stream restarts with safety limit
            stream_start_time = asyncio.get_event_loop().time()
            stream_ended = False
            restart_count += 1
            
            logging.info(f"Starting Google STT stream (restart #{restart_count})")
            
            async def audio_generator():
                """Async generator feeding Google STT with initial config then audio frames."""
                nonlocal stream_ended
                # Send config first
                yield speech.StreamingRecognizeRequest(streaming_config=STREAMING_CONFIG)
                
                while True:
                    # Check if WebSocket is still connected
                    if websocket.client_state.name != 'CONNECTED':
                        logging.info("WebSocket disconnected, ending audio stream")
                        stream_ended = True
                        break
                    
                    # Check if we need to end this stream due to duration limit
                    elapsed = asyncio.get_event_loop().time() - stream_start_time
                    if elapsed >= MAX_STREAM_DURATION:
                        logging.info(f"Stream duration limit reached ({elapsed:.1f}s), ending stream for restart")
                        stream_ended = True
                        break
                    
                    try:
                        # Use timeout to periodically check stream duration
                        chunk_b64 = await asyncio.wait_for(audio_queue.get(), timeout=1.0)
                        if chunk_b64 is None:
                            # WebSocket closed (None is pushed by receive_from_twilio on disconnect)
                            logging.info("Received None from audio queue, ending stream")
                            stream_ended = True
                            break
                        # Twilio sends base64 PCMU frames; decode to raw bytes for Google STT
                        yield speech.StreamingRecognizeRequest(audio_content=base64.b64decode(chunk_b64))
                    except asyncio.TimeoutError:
                        # No audio received in 1 second, continue checking (normal for silence)
                        continue

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

                    # Handle interim results for interruption detection
                    if not result.is_final:
                        if transcript and len(transcript.strip()) > 0:
                            # User is speaking - interrupt current TTS and agent processing
                            current_time = latest_media_timestamp / 1000.0  # Convert to seconds
                            
                            # Only interrupt if we have a meaningful interim result and enough time has passed
                            if (current_time - last_interim_time) > 0.1:  # 100ms debounce
                                logging.info(f"User speaking (interim): {transcript} - interrupting TTS and agent")
                                interrupt_flag.set()
                                last_interim_time = current_time
                                
                                # Send clear command to stop current audio
                                await clear_twilio_audio(websocket, stream_sid)
                                
                                # Cancel current TTS task
                                if current_tts_task and not current_tts_task.done():
                                    current_tts_task.cancel()
                                
                                # Cancel current agent processing task
                                if current_agent_task and not current_agent_task.done():
                                    current_agent_task.cancel()
                                    logging.info("Cancelled ongoing agent processing due to interim speech")
                        continue

                    # Handle final results
                    if result.is_final:
                        # Clear interruption flag when we get a final result
                        interrupt_flag.clear()
                        
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

                        # Cancel any ongoing agent processing
                        if current_agent_task and not current_agent_task.done():
                            current_agent_task.cancel()
                            logging.info("Cancelled previous agent processing for new user input")

                        # Start new agent processing task
                        current_agent_task = asyncio.create_task(
                            process_agent_response(transcript, websocket, stream_sid, interrupt_flag)
                        )
                        
                        # Wait for the agent processing to complete or be interrupted
                        try:
                            await current_agent_task
                        except asyncio.CancelledError:
                            logging.info("Agent processing was cancelled")
                        except Exception as e:
                            logging.error(f"Error in agent processing task: {e}")
                            logging.error(traceback.format_exc())

            except Exception as e:
                if "OutOfRange" in str(type(e).__name__) or "Exceeded maximum allowed stream duration" in str(e):
                    logging.warning(f"Stream duration limit reached, restarting stream: {e}")
                    # Continue to restart the stream
                else:
                    logging.error(f"Error during Google STT processing: {e}")
                    logging.error(traceback.format_exc())
                    # For other errors, break the loop
                    break
            
            # Check if we should restart the stream or exit
            if stream_ended:
                # Check if WebSocket is still connected
                if websocket.client_state.name != 'CONNECTED':
                    logging.info("WebSocket closed, exiting STT processing")
                    break
                
                # Check if call is ended
                if call_sid:
                    call_info = get_call_session(call_sid)
                    if call_info and call_info.get("status") in ["disconnected", "ended", "error"]:
                        logging.info(f"Call {call_sid} ended, exiting STT processing")
                        break
                
                # Check if we've hit the restart limit
                if restart_count >= MAX_RESTARTS:
                    logging.warning(f"Maximum restart limit ({MAX_RESTARTS}) reached, ending STT processing")
                    break
                
                # Otherwise, restart the stream
                logging.info(f"Restarting Google STT stream to continue session (restart {restart_count}/{MAX_RESTARTS})")
                await asyncio.sleep(0.1)  # Brief pause before restart
                continue
            else:
                # Unexpected exit, break the loop
                logging.info("Stream ended unexpectedly, exiting STT processing")
                break
        
        logging.info("STT processing finished.")

    # Run receiver and agent/STT tasks concurrently
    try:
        await asyncio.gather(receive_from_twilio(), run_agent_and_send_response())
    finally:
        # Update call status when WebSocket closes
        if call_sid:
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
    uvicorn.run("twilio_integration_outbound:app", host="0.0.0.0", port=5050, loop="uvloop", http="httptools", ws="websockets")

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

