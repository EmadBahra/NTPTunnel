#!/usr/bin/env python3
import argparse
import asyncio
import multiprocessing as mp
import os
import socket
import struct
import time
from typing import Optional

NTP_STRUCT = struct.Struct("!BBBb11I")
NTP_EPOCH = 2208988800
MAX_PAYLOAD = 468
BATCH_SIZE = 8


def build_ntp_packet(payload: bytes) -> bytes:
    now = time.time() + NTP_EPOCH
    seconds = int(now)
    fraction = int((now - seconds) * (1 << 32))

    li_vn_mode = (0 << 6) | (4 << 3) | 4
    stratum = 1
    poll = 4
    precision = -20

    root_delay = 0
    root_dispersion = 0
    ref_id = 0
    ref_ts_sec = seconds
    ref_ts_frac = fraction
    orig_ts_sec = 0
    orig_ts_frac = 0
    recv_ts_sec = 0
    recv_ts_frac = 0

    p = payload[:8].ljust(8, b"\0")
    xmit_ts_sec = int.from_bytes(p[:4], "big")
    xmit_ts_frac = int.from_bytes(p[4:], "big")

    header = NTP_STRUCT.pack(
        li_vn_mode,
        stratum,
        poll,
        precision,
        root_delay,
        root_dispersion,
        ref_id,
        ref_ts_sec,
        ref_ts_frac,
        orig_ts_sec,
        orig_ts_frac,
        recv_ts_sec,
        recv_ts_frac,
        xmit_ts_sec,
        xmit_ts_frac,
    )
    return header


def extract_payload(packet: bytes) -> bytes:
    if len(packet) < NTP_STRUCT.size:
        return b""
    fields = NTP_STRUCT.unpack(packet[:NTP_STRUCT.size])
    xmit_ts_sec = fields[-2]
    xmit_ts_frac = fields[-1]
    return xmit_ts_sec.to_bytes(4, "big") + xmit_ts_frac.to_bytes(4, "big")


class UdpTunnelEndpoint(asyncio.DatagramProtocol):
    def __init__(self, loop: asyncio.AbstractEventLoop, tcp_reader: asyncio.StreamReader,
                 tcp_writer: asyncio.StreamWriter, is_server: bool):
        self.loop = loop
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.tcp_reader = tcp_reader
        self.tcp_writer = tcp_writer
        self.is_server = is_server
        self.send_queue = asyncio.Queue()
        self.sender_task: Optional[asyncio.Task] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport
        self.sender_task = self.loop.create_task(self._sender_loop())

    def datagram_received(self, data: bytes, addr) -> None:
        payload = extract_payload(data)
        if payload:
            self.loop.create_task(self._forward_to_tcp(payload))

    def error_received(self, exc: Exception) -> None:
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if self.sender_task:
            self.sender_task.cancel()

    async def _forward_to_tcp(self, payload: bytes) -> None:
        try:
            self.tcp_writer.write(payload)
            await self.tcp_writer.drain()
        except ConnectionError:
            if self.transport:
                self.transport.close()

    async def _sender_loop(self) -> None:
        try:
            while True:
                batch = []
                item = await self.send_queue.get()
                batch.append(item)
                for _ in range(BATCH_SIZE - 1):
                    try:
                        item = self.send_queue.get_nowait()
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break

                if not self.transport:
                    continue

                for payload in batch:
                    pkt = build_ntp_packet(payload)
                    self.transport.sendto(pkt)
        except asyncio.CancelledError:
            pass

    async def pump_tcp_to_udp(self) -> None:
        try:
            while True:
                data = await self.tcp_reader.read(MAX_PAYLOAD)
                if not data:
                    break
                await self.send_queue.put(data)
        except ConnectionError:
            pass
        finally:
            if self.transport:
                self.transport.close()
            self.tcp_writer.close()
            await self.tcp_writer.wait_closed()


async def handle_tcp_connection(loop, reader, writer, ntp_addr, is_server: bool, reuse_port: bool):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if reuse_port and hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.setblocking(False)

    if is_server:
        sock.bind(ntp_addr)
        remote_addr = None
    else:
        sock.bind(("0.0.0.0", 0))
        remote_addr = ntp_addr

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UdpTunnelEndpoint(loop, reader, writer, is_server=is_server),
        sock=sock,
        remote_addr=remote_addr,
    )

    await protocol.pump_tcp_to_udp()
    transport.close()


async def run_server_worker(ntp_port: int, tcp_port: int, reuse_port: bool):
    loop = asyncio.get_event_loop()

    async def tcp_handler(reader, writer):
        await handle_tcp_connection(
            loop,
            reader,
            writer,
            ("0.0.0.0", ntp_port),
            is_server=True,
            reuse_port=reuse_port,
        )

    server = await asyncio.start_server(
        tcp_handler,
        host="0.0.0.0",
        port=tcp_port,
        reuse_port=reuse_port if hasattr(socket, "SO_REUSEPORT") else False,
    )
    async with server:
        await server.serve_forever()


def server_worker_main(ntp_port: int, tcp_port: int, reuse_port: bool):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_server_worker(ntp_port, tcp_port, reuse_port))


async def run_client(loop, server_host, server_ntp_port, local_tcp_port):
    async def client_handler(reader, writer):
        await handle_tcp_connection(
            loop,
            reader,
            writer,
            (server_host, server_ntp_port),
            is_server=False,
            reuse_port=False,
        )

    server = await asyncio.start_server(
        client_handler,
        host="127.0.0.1",
        port=local_tcp_port,
    )
    async with server:
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Async NTP tunnel (multi-worker).")
    sub = parser.add_subparsers(dest="mode", required=True)

    srv = sub.add_parser("server", help="Run in server mode")
    srv.add_argument("--ntp-port", type=int, default=123, help="UDP port to listen on")
    srv.add_argument("--tcp-port", type=int, default=8080, help="TCP port to expose")
    srv.add_argument("--workers", type=int, default=os.cpu_count() or 2,
                     help="Number of worker processes")
    srv.add_argument("--no-reuseport", action="store_true",
                     help="Disable SO_REUSEPORT (for platforms that misbehave)")

    cli = sub.add_parser("client", help="Run in client mode")
    cli.add_argument("server_host", help="Server hostname/IP")
    cli.add_argument("--server-ntp-port", type=int, default=123, help="Server NTP UDP port")
    cli.add_argument("--local-tcp-port", type=int, default=8080, help="Local TCP port to expose")

    args = parser.parse_args()

    if args.mode == "server":
        reuse_port = not args.no_reuseport and hasattr(socket, "SO_REUSEPORT")
        procs = []
        for _ in range(args.workers):
            p = mp.Process(
                target=server_worker_main,
                args=(args.ntp_port, args.tcp_port, reuse_port),
                daemon=True,
            )
            p.start()
            procs.append(p)

        try:
            for p in procs:
                p.join()
        except KeyboardInterrupt:
            for p in procs:
                p.terminate()
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            run_client(loop, args.server_host, args.server_ntp_port, args.local_tcp_port)
        )


if __name__ == "__main__":
    main()
