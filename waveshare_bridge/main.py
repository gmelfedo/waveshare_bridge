import os
import sys
import json
import asyncio
from pymodbus.client import AsyncModbusTcpClient
import paho.mqtt.client as mqtt

OPTIONS_FILE = "/data/options.json"
DATA_FILE = "/data/timer_config.json"

modbus_client = None
mqtt_client_global = None
main_loop = None  # <--- AGGIUNGI QUESTO

# ==============================================================================
# 1. CARICAMENTO CONFIGURAZIONI (IBRIDO: ADD-ON / ENV PORTAINER)
# ==============================================================================
if os.path.exists(OPTIONS_FILE):
    print("[Core] Rilevato ambiente Home Assistant Add-on. Caricamento opzioni da UI...")
    with open(OPTIONS_FILE, "r") as f:
        options = json.load(f)
    
    POLLING_RATE_MS = int(options.get("polling_rate_ms", 200))
    MODBUS_IP = options.get("modbus_ip")
    MODBUS_PORT = int(options.get("modbus_port", 4196))
    DEVICE_NAME = options.get("device_name", "Sesamo Autisti")
    MQTT_BROKER = options.get("mqtt_broker", "core-mosquitto")
    MQTT_PORT = int(options.get("mqtt_port", 1883))
    MQTT_USER = options.get("mqtt_user")
    MQTT_PASSWORD = options.get("mqtt_password")
    # Controllo LOG disattivabile da UI Add-on (default: true)
    DEBUG_LOG = str(options.get("debug_log", "true")).lower() == "true"
else:
    print("[Core] Ambiente standard rilevato. Caricamento opzioni da ENV...")

    POLLING_RATE_MS = int(os.getenv("POLLING_RATE_MS", 200))
    MODBUS_IP = os.getenv("MODBUS_IP")
    MODBUS_PORT = int(os.getenv("MODBUS_PORT", 4196))
    DEVICE_NAME = os.getenv("DEVICE_NAME", "Sesamo Autisti")
    MQTT_BROKER = os.getenv("MQTT_BROKER")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
    MQTT_USER = os.getenv("MQTT_USER")
    MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
    # Controllo LOG disattivabile da Compose ENV (default: true)
    DEBUG_LOG = os.getenv("DEBUG_LOG", "true").lower() == "true"

DEVICE_ID_CLEAN = DEVICE_NAME.lower().replace(" ", "_")

if not MODBUS_IP or not MQTT_BROKER:
    print("\n" + "!"*60 + "\n ERRORE CRITICO: Configurazione incompleta!\n" + "!"*60 + "\n")
    sys.exit(1)

# Lo Slave ID (Unit ID) per la scheda Waveshare di solito è 1
SLAVE_ID = 1

# ==============================================================================
# 2. STATO INTERNO & PERSISTENZA TIMER
# ==============================================================================
DEFAULT_TIMER_MS = 500
relay_timer_config = {}      # { relay_id: ms_int }
active_timers = {}           # { relay_id: asyncio.Task }
last_inputs_state = [False] * 8

modbus_client = None
mqtt_client_global = None

def carica_timer():
    global relay_timer_config
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                dati_caricati = json.load(f)
                relay_timer_config = {int(k): int(v) for k, v in dati_caricati.items()}
            print(f"[Persistenza] Timer caricati da archivio: {relay_timer_config}")
        except Exception as e:
            print(f"[Persistenza] Errore lettura file di storage: {e}")
            relay_timer_config = {}
    else:
        print(f"[Persistenza] Nessun file di storage trovato. Inizializzazione pulita.")
        relay_timer_config = {}

def salva_timer():
    try:
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, "w") as f:
            json.dump(relay_timer_config, f, indent=4)
        print("[Persistenza] Configurazione salvata in modo sicuro.")
    except Exception as e:
        print(f"[Persistenza] Errore durante il salvataggio dei dati: {e}")

carica_timer()

# ==============================================================================
# 3. LOGICA TIMER ENGINE (AUTO-OFF)
# ==============================================================================
async def auto_off_worker(relay_id, delay_seconds):
    try:
        await asyncio.sleep(delay_seconds)
        if DEBUG_LOG:
            print(f"[Scheda] Spengo relè {relay_id} (Auto-off)")
        await modbus_client.write_coil(relay_id - 1, value=False)
        mqtt_client_global.publish(f"waveshare/{DEVICE_ID_CLEAN}/switch/{relay_id}/state", "OFF", retain=True)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[Timer] Errore durante l'auto-off: {e}")

async def trigger_relay_logic(relay_id, turn_on: bool):
    address = relay_id - 1
    try:
        if DEBUG_LOG:
            azione = "Accendo" if turn_on else "Spengo"
            print(f"[Scheda] {azione} relè {relay_id}")
            
        await modbus_client.write_coil(address, value=turn_on)
        
        if turn_on:
            if relay_id in active_timers and not active_timers[relay_id].done():
                active_timers[relay_id].cancel()
                
            ms_delay = relay_timer_config.get(relay_id, DEFAULT_TIMER_MS)
            if ms_delay > 0:
                task = asyncio.create_task(auto_off_worker(relay_id, ms_delay / 1000.0))
                active_timers[relay_id] = task
        else:
            if relay_id in active_timers and not active_timers[relay_id].done():
                active_timers[relay_id].cancel()
    except Exception as e:
        print(f"[Modbus] Errore durante la scrittura del coil {address}: {e}")

# ==============================================================================
# 4. GESTIONE MQTT & DISCOVERY
# ==============================================================================
def invia_discovery_home_assistant(client):
    device_info = {
        "identifiers": [f"waveshare_{DEVICE_ID_CLEAN}"],
        "name": DEVICE_NAME,
        "model": "8-CH Modbus Board",
        "manufacturer": "Waveshare"
    }
    for i in range(1, 9):
        client.publish(f"homeassistant/switch/{DEVICE_ID_CLEAN}_relay_{i}/config", json.dumps({
            "name": f"Relè {i}", "unique_id": f"{DEVICE_ID_CLEAN}_relay_{i}",
            "state_topic": f"waveshare/{DEVICE_ID_CLEAN}/switch/{i}/state",
            "command_topic": f"waveshare/{DEVICE_ID_CLEAN}/switch/{i}/set",
            "payload_on": "ON", "payload_off": "OFF", "device": device_info
        }), retain=True)

        client.publish(f"homeassistant/binary_sensor/{DEVICE_ID_CLEAN}_input_{i}/config", json.dumps({
            "name": f"Ingresso {i}", "unique_id": f"{DEVICE_ID_CLEAN}_input_{i}",
            "state_topic": f"waveshare/{DEVICE_ID_CLEAN}/binary_sensor/{i}/state",
            "payload_on": "ON", "payload_off": "OFF", "device": device_info
        }), retain=True)

        client.publish(f"homeassistant/number/{DEVICE_ID_CLEAN}_timer_{i}/config", json.dumps({
            "name": f"Timer Relè {i}", "unique_id": f"{DEVICE_ID_CLEAN}_timer_{i}",
            "state_topic": f"waveshare/{DEVICE_ID_CLEAN}/timer/{i}/state",
            "command_topic": f"waveshare/{DEVICE_ID_CLEAN}/timer/{i}/set",
            "min": 0, "max": 60000, "step": 50, "unit_of_measurement": "ms", "mode": "box", "device": device_info
        }), retain=True)
        
        valore_attuale = relay_timer_config.get(i, DEFAULT_TIMER_MS)
        client.publish(f"waveshare/{DEVICE_ID_CLEAN}/timer/{i}/state", str(valore_attuale), retain=True)
        
    print(f"[MQTT] Discovery inviata. Dispositivo '{DEVICE_NAME}' sincronizzato.")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[MQTT] Connesso con successo a {MQTT_BROKER}")
        invia_discovery_home_assistant(client)
        client.subscribe(f"waveshare/{DEVICE_ID_CLEAN}/switch/+/set")
        client.subscribe(f"waveshare/{DEVICE_ID_CLEAN}/timer/+/set")
    else:
        print(f"[MQTT] Connessione fallita con codice: {rc}"); sys.exit(1)

def on_message(client, userdata, msg):
    global main_loop
    topic = msg.topic
    payload = msg.payload.decode("utf-8")
    parts = topic.split("/")
    
    if "switch" in topic:
        relay_id = int(parts[-2])
        turn_on = (payload == "ON")
        
        # CORREZIONE: Verifichiamo che il loop principale sia attivo e usiamo quello
        if main_loop and main_loop.is_running():
            asyncio.run_coroutine_threadsafe(trigger_relay_logic(relay_id, turn_on), main_loop)
        else:
            print("[MQTT] Errore: Event loop principale non disponibile!")
            
        client.publish(f"waveshare/{DEVICE_ID_CLEAN}/switch/{relay_id}/state", payload, retain=True)
        
    elif "timer" in topic:
        relay_id = int(parts[-2])
        try:
            val_ms = int(payload)
            relay_timer_config[relay_id] = val_ms
            if DEBUG_LOG:
                print(f"[Timer] Relè {relay_id} modificato a {val_ms}ms")
            salva_timer()
            client.publish(f"waveshare/{DEVICE_ID_CLEAN}/timer/{relay_id}/state", str(val_ms), retain=True)
        except ValueError:
            pass

# ==============================================================================
# 5. CORE ASINCRONO & MODBUS POLLING
# ==============================================================================
async def main():
    global modbus_client, mqtt_client_global, main_loop
    
    # Memorizza il loop del thread principale prima di avviare il loop MQTT
    main_loop = asyncio.get_running_loop()
    
    # Setup MQTT
    print(f"[MQTT] Connessione a {MQTT_BROKER}:{MQTT_PORT}...")

    mqtt_client_global = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USER and MQTT_PASSWORD:
        mqtt_client_global.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client_global.on_connect = on_connect
    mqtt_client_global.on_message = on_message

    try:
        mqtt_client_global.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client_global.loop_start()
    except Exception as e:
        print(f"[MQTT] ERRORE: Impossibile connettersi: {e}"); sys.exit(1)

    print(f"[Modbus] Connessione a {MODBUS_IP}:{MODBUS_PORT}...")
    modbus_client = AsyncModbusTcpClient(MODBUS_IP, port=MODBUS_PORT)
    
    try:
        connected = await modbus_client.connect()
        if not connected:
            print(f"[Modbus] ERRORE: Connessione rifiutata da {MODBUS_IP}"); sys.exit(1)
        print("[Modbus] Connessione stabilita.")
    except Exception as e:
        print(f"[Modbus] ERRORE: Scheda irraggiungibile: {e}"); sys.exit(1)

    global last_inputs_state
    print("[Core] Polling avviato.")
    
    try:
        while True:
            try:
                result = await modbus_client.read_discrete_inputs(0, count=8)
                
                if result and not result.isError():
                    current_states = result.bits[:8]
                    for i in range(8):
                        if current_states[i] != last_inputs_state[i]:
                            state_str = "ON" if current_states[i] else "OFF"
                            # Mappatura log su richiesta: ON -> UP, OFF -> DOWN
                            log_state_str = "UP" if current_states[i] else "DOWN"
                            input_num = i + 1
                            
                            if DEBUG_LOG:
                                print(f"[Input] ingresso {input_num} {log_state_str}")
                                
                            mqtt_client_global.publish(
                                f"waveshare/{DEVICE_ID_CLEAN}/binary_sensor/{input_num}/state", 
                                state_str, 
                                retain=True
                            )
                    last_inputs_state = current_states
            except Exception as modbus_err:
                print(f"[Modbus] Errore di lettura temporaneo: {modbus_err}")
                
            await asyncio.sleep(POLLING_RATE_MS / 1000.0)
            
    except asyncio.CancelledError:
        print("[Core] In fase di arresto...")
    finally:
        mqtt_client_global.loop_stop()
        mqtt_client_global.disconnect()
        await modbus_client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)