#!/usr/bin/env python3
"""最小的 xiaozhi websocket 协议自测（不需要 opus/音频，只验证 hello/listen/ping/abort）"""
import asyncio, json, sys
import websockets

URL = sys.argv[1] if len(sys.argv) > 1 else "ws://127.0.0.1:8000/xiaozhi/v1/"

async def main():
    async with websockets.connect(URL, additional_headers={"device-id": "aa:bb:cc:dd:ee:ff", "client-id": "test-client"}) as ws:
        await ws.send(json.dumps({
            "type": "hello", "version": 1, "transport": "websocket",
            "features": {"aec": False, "mcp": False},
            "audio_params": {"format": "opus", "sample_rate": 24000, "channels": 1, "frame_duration": 60},
        }))
        welcome = json.loads(await asyncio.wait_for(ws.recv(), 5))
        print("[welcome]", json.dumps(welcome, ensure_ascii=False))
        assert welcome.get("type") == "hello", welcome
        assert welcome.get("session_id"), welcome
        assert welcome["audio_params"]["format"] == "opus"

        await ws.send(json.dumps({"type": "listen", "state": "start", "mode": "auto"}))
        await ws.send(json.dumps({"type": "ping"}))
        pong = json.loads(await asyncio.wait_for(ws.recv(), 5))
        print("[pong]", json.dumps(pong, ensure_ascii=False))
        assert pong.get("type") == "pong", pong

        await ws.send(json.dumps({"type": "abort"}))
        print("[ok] 协议自测通过")

asyncio.run(main())
