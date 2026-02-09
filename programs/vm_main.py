#!/usr/bin/env python3
"""
Point d'entrée principal du script VM
Gère la connexion au serveur, la réception du fichier .blend,
le lancement de Blender et le streaming du cache
"""

import asyncio
import logging
import sys
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError

from config import Config
from utils import setup_logging, decode_base64_to_file
from ws_client import WSClient
from cache_streamer import CacheStreamer
from blender_runner import BlenderRunner

logger = logging.getLogger(__name__)

# État global
ws_client: WSClient = None
cache_streamer: CacheStreamer = None
blender_runner: BlenderRunner = None
heartbeat_task: asyncio.Task = None
shutdown_event = asyncio.Event()


async def heartbeat_loop():
    """Envoie des heartbeats réguliers au serveur"""
    logger.info(f"Démarrage heartbeat (interval: {Config.HEARTBEAT_INTERVAL}s)")

    try:
        while not shutdown_event.is_set():
            if ws_client and ws_client.is_connected():
                await ws_client.send_heartbeat()

            await asyncio.sleep(Config.HEARTBEAT_INTERVAL)

    except asyncio.CancelledError:
        logger.info("Heartbeat loop annulée")


async def on_authenticated(message: dict):
    """Callback appelé après authentification réussie"""
    has_cache = message.get('hasCache', False)

    logger.info(f"Authentifié. Cache disponible: {has_cache}")

    global heartbeat_task
    heartbeat_task = asyncio.create_task(heartbeat_loop())


async def on_message(message: dict):
    """Callback appelé à la réception d'un message"""
    msg_type = message.get('type')

    if msg_type == 'BLEND_FILE_URL':
        await handle_blend_file_url(message)

    elif msg_type == 'BLEND_FILE':
        # Compatibilité avec l'ancien format base64 (petits fichiers)
        await handle_blend_file_base64(message)

    elif msg_type == 'CACHE_DATA':
        await handle_cache_data(message)

    elif msg_type == 'CACHE_DATA_URL':
        await handle_cache_data_url(message)

    elif msg_type == 'TERMINATE':
        reason = message.get('reason', 'Non spécifié')
        logger.warning(f"Demande de terminaison: {reason}")
        await shutdown()


async def handle_blend_file_url(message: dict):
    """Télécharge le fichier .blend depuis une URL pré-signée"""
    url = message.get('url')
    name = message.get('name', 'current.blend')
    size = message.get('size', 0)

    if not url:
        logger.error("Pas d'URL dans le message BLEND_FILE_URL")
        return

    logger.info(f"Téléchargement .blend depuis URL pré-signée: {name} ({size} bytes)")

    try:
        # Téléchargement via urllib (pas de dépendance externe nécessaire)
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, _download_url, url)

        Config.BLEND_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(Config.BLEND_FILE, 'wb') as f:
            f.write(data)

        logger.info(f"Fichier .blend sauvegardé: {Config.BLEND_FILE} ({len(data)} bytes)")

        asyncio.create_task(start_blender())

    except Exception as e:
        logger.error(f"Erreur téléchargement .blend: {e}", exc_info=True)


def _download_url(url: str) -> bytes:
    """Télécharge une URL (exécuté dans un executor)"""
    try:
        with urlopen(url, timeout=300) as response:
            return response.read()
    except URLError as e:
        raise RuntimeError(f"Erreur téléchargement: {e}")


async def handle_blend_file_base64(message: dict):
    """Gère la réception du fichier .blend en base64 (fallback)"""
    logger.info("Réception du fichier .blend (base64)...")

    try:
        data = message.get('data')
        name = message.get('name', 'current.blend')
        size = message.get('size', 0)

        if not data:
            logger.error("Pas de données dans le message BLEND_FILE")
            return

        decode_base64_to_file(data, Config.BLEND_FILE)

        logger.info(f"Fichier .blend sauvegardé: {name} ({size} bytes)")

        asyncio.create_task(start_blender())

    except Exception as e:
        logger.error(f"Erreur traitement BLEND_FILE: {e}", exc_info=True)


async def handle_cache_data(message: dict):
    """Gère la réception de données de cache"""
    logger.info("Réception de données de cache...")

    try:
        data = message.get('data')

        if not data:
            logger.error("Pas de données dans le message CACHE_DATA")
            return

        cache_file = Config.CACHE_DIR / 'received_cache.bin'
        decode_base64_to_file(data, cache_file)

        logger.info(f"Cache sauvegardé: {cache_file}")

    except Exception as e:
        logger.error(f"Erreur traitement CACHE_DATA: {e}", exc_info=True)


async def handle_cache_data_url(message: dict):
    """Télécharge le cache depuis une URL pré-signée"""
    url = message.get('url')
    if not url:
        logger.error("Pas d'URL dans le message CACHE_DATA_URL")
        return

    logger.info("Téléchargement cache depuis URL pré-signée...")

    try:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, _download_url, url)

        cache_file = Config.CACHE_DIR / 'received_cache.bin'
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'wb') as f:
            f.write(data)

        logger.info(f"Cache téléchargé: {cache_file} ({len(data)} bytes)")

    except Exception as e:
        logger.error(f"Erreur téléchargement cache: {e}", exc_info=True)


async def start_blender():
    """Démarre Blender et le streaming du cache"""
    global cache_streamer, blender_runner

    await asyncio.sleep(2.0)

    logger.info("Démarrage de Blender et du cache streamer...")

    try:
        cache_streamer = CacheStreamer(Config.CACHE_DIR, ws_client)
        cache_streamer.start()

        blender_runner = BlenderRunner(Config.BLEND_FILE, Config.CACHE_DIR)
        return_code = await blender_runner.run()

        logger.info(f"Blender terminé avec le code: {return_code}")

        await cache_streamer.finalize()

        stats = cache_streamer.get_stats()
        logger.info(f"Stats streaming: {stats}")

        await ws_client.send_ready_to_terminate()

    except Exception as e:
        logger.error(f"Erreur démarrage Blender: {e}", exc_info=True)
    finally:
        if cache_streamer:
            cache_streamer.stop()


async def shutdown():
    """Arrêt propre de l'application"""
    logger.info("Arrêt en cours...")
    shutdown_event.set()

    if blender_runner and blender_runner.is_alive():
        blender_runner.terminate(graceful=True)

    if cache_streamer:
        cache_streamer.stop()

    if heartbeat_task and not heartbeat_task.done():
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

    if ws_client:
        ws_client.disconnect()

    logger.info("Arrêt terminé")


async def main():
    """Point d'entrée principal"""
    global ws_client

    setup_logging(logging.INFO)

    logger.info("=" * 60)
    logger.info("Blender VM Worker - Démarrage")
    logger.info("=" * 60)

    try:
        Config.validate()
        logger.info("Configuration validée")
        logger.info(f"URL WebSocket: {Config.WS_URL}")
        logger.info(f"Répertoire de travail: {Config.WORK_DIR}")
        logger.info(f"Répertoire cache: {Config.CACHE_DIR}")

        # Configure les signal handlers via le loop (compatible asyncio)
        loop = asyncio.get_running_loop()
        try:
            import signal
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
        except (NotImplementedError, AttributeError):
            # Windows : add_signal_handler n'est pas supporté
            # Le KeyboardInterrupt sera attrapé par le try/except
            pass

        ws_client = WSClient(Config.WS_URL, Config.VM_PASSWORD)
        ws_client.on_authenticated = on_authenticated
        ws_client.on_message = on_message
        ws_client.on_disconnected = lambda: logger.warning("Déconnecté du serveur")
        ws_client.on_error = lambda e: logger.error(f"Erreur WebSocket: {e}")

        await ws_client.connect()

        await shutdown_event.wait()

    except KeyboardInterrupt:
        logger.info("Interruption clavier")
        await shutdown()

    except Exception as e:
        logger.error(f"Erreur fatale: {e}", exc_info=True)
        await shutdown()
        return 1

    logger.info("Terminé")
    return 0


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)