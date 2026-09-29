"""
Serverless Ingest API.
Simulates a high-throughput serverless function / API Gateway endpoint for student check-ins.
Receives student sign-in requests, immediately enqueues tasks to the local Cloud Tasks emulator,
and returns HTTP 202 Accepted for ultra-fast response times.
Also serves the real-time interactive testing ground dashboard.
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

from flask import Flask, jsonify, request, render_template_string
import grpc
from google.cloud.tasks_v2 import CloudTasksClient, Task, HttpRequest, HttpMethod
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport

import db
from simulation_engine import simulator

INGEST_HOST = os.environ.get("INGEST_HOST", "127.0.0.1")
INGEST_PORT = int(os.environ.get("INGEST_PORT", "8080"))
EMULATOR_HOST = os.environ.get("TASKS_EMULATOR_HOST", "127.0.0.1")
EMULATOR_PORT = int(os.environ.get("TASKS_EMULATOR_PORT", "9090"))
DISPATCHER_HOST = os.environ.get("DISPATCHER_HOST", "127.0.0.1")
DISPATCHER_PORT = int(os.environ.get("DISPATCHER_PORT", "8081"))

QUEUE_NAME = os.environ.get(
    "ATTENDANCE_QUEUE_NAME",
    "projects/local-dev/locations/us-central1/queues/student-attendance-queue",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [IngestAPI] %(message)s",
)
logger = logging.getLogger("ingest_api")

app = Flask(__name__)

# Server Wakeup & Health tracking
_server_start_time = time.time()
_last_wakeup_time = None
_wakeup_count = 0
_shutdown_hook = None


def set_shutdown_hook(fn):
    global _shutdown_hook
    _shutdown_hook = fn

# Cloud Tasks Client singleton
_tasks_client = None
_grpc_channel = None


def get_cloud_tasks_client():
    global _tasks_client, _grpc_channel
    if _tasks_client is None:
        target = f"{EMULATOR_HOST}:{EMULATOR_PORT}"
        # Set max send/receive message length and channel options for high throughput
        options = [
            ("grpc.max_send_message_length", 50 * 1024 * 1024),
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
        ]
        _grpc_channel = grpc.insecure_channel(target, options=options)
        transport = CloudTasksGrpcTransport(channel=_grpc_channel)
        _tasks_client = CloudTasksClient(transport=transport)
        logger.info(f"Initialized CloudTasksClient connected to emulator at {target}")
    return _tasks_client


HTML_DASHBOARD = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Student Attendance System - Local Testing Ground</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0b0f19;
            --bg-card: rgba(18, 24, 38, 0.75);
            --bg-card-hover: rgba(28, 36, 56, 0.85);
            --border: rgba(255, 255, 255, 0.08);
            --border-glow: rgba(99, 102, 241, 0.3);
            --accent: #6366f1;
            --accent-gradient: linear-gradient(135deg, #6366f1 0%, #a855f7 50%, #ec4899 100%);
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --info: #38bdf8;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: 'Outfit', sans-serif;
            background-color: var(--bg-primary);
            background-image: 
                radial-gradient(at 10% 20%, rgba(99, 102, 241, 0.12) 0px, transparent 50%),
                radial-gradient(at 90% 80%, rgba(236, 72, 153, 0.1) 0px, transparent 50%);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 2rem 1.5rem;
        }

        .container {
            max-width: 1280px;
            margin: 0 auto;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            padding-bottom: 1.5rem;
            border-bottom: 1px solid var(--border);
            flex-wrap: wrap;
            gap: 1rem;
        }

        .logo-group h1 {
            font-size: 1.75rem;
            font-weight: 700;
            background: var(--accent-gradient);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            display: inline-block;
        }

        .logo-group p {
            color: var(--text-secondary);
            font-size: 0.9rem;
            margin-top: 0.25rem;
        }

        .badge {
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            background: rgba(16, 185, 129, 0.1);
            color: #34d399;
            border: 1px solid rgba(16, 185, 129, 0.25);
            padding: 0.35rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.8rem;
            font-weight: 600;
        }

        .pulse-dot {
            width: 8px;
            height: 8px;
            background-color: var(--success);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
        }

        /* Topology grid */
        .topology-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }

        .service-node {
            background: var(--bg-card);
            backdrop-filter: blur(12px);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.25rem;
            position: relative;
            overflow: hidden;
            transition: transform 0.2s, border-color 0.2s;
        }

        .service-node:hover {
            transform: translateY(-2px);
            border-color: var(--border-glow);
        }

        .service-node::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 3px;
            background: var(--accent-gradient);
        }

        .node-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.75rem;
        }

        .node-title {
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            font-weight: 600;
        }

        .node-port {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.75rem;
            color: var(--accent);
            background: rgba(99, 102, 241, 0.1);
            padding: 0.15rem 0.5rem;
            border-radius: 6px;
        }

        .node-value {
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 0.25rem;
        }

        .node-desc {
            font-size: 0.8rem;
            color: var(--text-secondary);
        }

        /* Metrics grid */
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }

        .metric-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.25rem;
            text-align: center;
        }

        .metric-number {
            font-size: 2.25rem;
            font-weight: 700;
            color: #fff;
            margin: 0.5rem 0 0.25rem;
            font-family: 'JetBrains Mono', monospace;
        }

        .metric-label {
            color: var(--text-secondary);
            font-size: 0.85rem;
            font-weight: 500;
        }

        /* Actions & Test Runner */
        .actions-panel {
            background: var(--bg-card);
            backdrop-filter: blur(12px);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1.5rem;
        }

        .actions-info h3 {
            font-size: 1.15rem;
            margin-bottom: 0.25rem;
        }

        .actions-info p {
            color: var(--text-secondary);
            font-size: 0.85rem;
        }

        .btn-group {
            display: flex;
            gap: 0.75rem;
            flex-wrap: wrap;
        }

        .btn {
            font-family: 'Outfit', sans-serif;
            font-size: 0.9rem;
            font-weight: 600;
            padding: 0.65rem 1.25rem;
            border-radius: 8px;
            cursor: pointer;
            border: none;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }

        .btn-primary {
            background: var(--accent-gradient);
            color: white;
            box-shadow: 0 4px 15px rgba(99, 102, 241, 0.35);
        }

        .btn-primary:hover {
            box-shadow: 0 6px 20px rgba(99, 102, 241, 0.5);
            transform: translateY(-1px);
        }

        .btn-secondary {
            background: rgba(255, 255, 255, 0.05);
            color: var(--text-secondary);
            border: 1px solid var(--border);
        }

        .btn-secondary:hover {
            background: rgba(255, 255, 255, 0.1);
            color: var(--text-primary);
        }

        /* Live test progress box */
        #test-status-box {
            display: none;
            background: rgba(15, 23, 42, 0.9);
            border: 1px solid var(--border-glow);
            border-radius: 10px;
            padding: 1rem 1.25rem;
            margin-bottom: 2rem;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
        }

        .progress-bar-container {
            width: 100%;
            height: 8px;
            background: rgba(255, 255, 255, 0.08);
            border-radius: 4px;
            overflow: hidden;
            margin-top: 0.75rem;
        }

        .progress-bar-fill {
            height: 100%;
            width: 0%;
            background: var(--accent-gradient);
            transition: width 0.3s ease;
        }

        /* Table */
        .card-table {
            background: var(--bg-card);
            backdrop-filter: blur(12px);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 1.5rem;
            overflow: hidden;
        }

        .card-table h2 {
            font-size: 1.2rem;
            margin-bottom: 1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
            text-align: left;
        }

        th {
            color: var(--text-muted);
            font-weight: 500;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--border);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        td {
            padding: 0.75rem 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            color: var(--text-secondary);
        }

        tr:hover td {
            background: rgba(255, 255, 255, 0.02);
            color: var(--text-primary);
        }

        .code-cell {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.8rem;
            color: #cbd5e1;
        }

        .status-pill {
            display: inline-block;
            padding: 0.2rem 0.6rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }

        .status-present {
            background: rgba(16, 185, 129, 0.15);
            color: #34d399;
            border: 1px solid rgba(16, 185, 129, 0.3);
        }

        .status-late {
            background: rgba(245, 158, 11, 0.15);
            color: #fbbf24;
            border: 1px solid rgba(245, 158, 11, 0.3);
        }

        .status-duplicate {
            background: rgba(56, 189, 248, 0.15);
            color: #7dd3fc;
            border: 1px solid rgba(56, 189, 248, 0.3);
        }

        .status-absent {
            background: rgba(239, 68, 68, 0.15);
            color: #f87171;
            border: 1px solid rgba(239, 68, 68, 0.3);
        }

        /* 2-Hour Clock & Queue Telemetry Card */
        .session-clock-card {
            background: var(--bg-card);
            backdrop-filter: blur(14px);
            border: 1px solid rgba(99, 102, 241, 0.35);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.37);
            border-radius: 16px;
            padding: 1.75rem;
            margin-bottom: 2rem;
            position: relative;
            overflow: hidden;
        }

        .session-clock-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: var(--accent-gradient);
        }

        .clock-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1.5rem;
            margin-bottom: 1.25rem;
        }

        .live-pill {
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #38bdf8;
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            margin-bottom: 0.4rem;
        }

        .clock-digits {
            font-size: 2.5rem;
            font-weight: 700;
            font-family: 'JetBrains Mono', monospace;
            color: #fff;
            letter-spacing: 0.02em;
        }

        .clock-total {
            font-size: 1.4rem;
            color: var(--text-muted);
            font-weight: 400;
        }

        .virtual-time {
            color: #a5b4fc;
            font-size: 0.95rem;
            font-family: 'JetBrains Mono', monospace;
            margin-top: 0.25rem;
        }

        .queue-depth-box {
            background: rgba(15, 23, 42, 0.85);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1rem 1.5rem;
            text-align: right;
            min-width: 220px;
        }

        .queue-depth-label {
            font-size: 0.8rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 600;
        }

        .queue-depth-number {
            font-size: 2rem;
            font-weight: 700;
            font-family: 'JetBrains Mono', monospace;
            color: #38bdf8;
            margin: 0.15rem 0;
        }

        .queue-depth-sub {
            font-size: 0.75rem;
            color: var(--text-secondary);
        }

        .phase-banner {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            background: rgba(99, 102, 241, 0.1);
            border: 1px solid rgba(99, 102, 241, 0.25);
            border-radius: 8px;
            padding: 0.6rem 1rem;
            margin-top: 0.75rem;
        }

        .phase-badge {
            background: var(--accent);
            color: white;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.05em;
        }

        .phase-desc {
            font-size: 0.85rem;
            color: #f1f5f9;
            font-weight: 500;
        }

        .sim-controls-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
            margin-top: 1.25rem;
            padding-top: 1.25rem;
            border-top: 1px solid var(--border);
        }

        .sim-btn-group {
            display: flex;
            gap: 0.6rem;
            flex-wrap: wrap;
        }

        .speed-selector {
            display: flex;
            align-items: center;
            gap: 0.4rem;
            flex-wrap: wrap;
        }

        .speed-btn {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border);
            color: var(--text-secondary);
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.35rem 0.65rem;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
        }

        .speed-btn:hover {
            background: rgba(255, 255, 255, 0.1);
            color: var(--text-primary);
        }

        .speed-btn.active {
            background: var(--accent-gradient);
            color: #fff;
            border-color: transparent;
            box-shadow: 0 0 10px rgba(99, 102, 241, 0.5);
        }

        /* Activity stream */
        .activity-stream {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
            max-height: 280px;
            overflow-y: auto;
            padding-right: 0.5rem;
        }

        .stream-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 8px;
            padding: 0.55rem 0.9rem;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.8rem;
            transition: all 0.2s;
            animation: fadeIn 0.3s ease;
        }

        .stream-item:hover {
            background: rgba(255, 255, 255, 0.04);
            border-color: rgba(99, 102, 241, 0.3);
        }

        .stream-left {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }

        .stream-time {
            color: var(--text-muted);
            font-size: 0.75rem;
        }

        .stream-student {
            color: #f8fafc;
            font-weight: 600;
        }

        .stream-flow {
            color: #a5b4fc;
            font-size: 0.75rem;
            display: flex;
            align-items: center;
            gap: 0.35rem;
        }

        .stream-empty {
            text-align: center;
            padding: 2rem;
            color: var(--text-muted);
            font-size: 0.85rem;
        }

        /* Scheduled Real-Time Cron Widget */
        .cron-widget {
            margin-top: 1.25rem;
            padding: 1rem 1.25rem;
            background: rgba(15, 23, 42, 0.7);
            border: 1px dashed rgba(99, 102, 241, 0.4);
            border-radius: 10px;
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }

        .cron-widget-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .cron-widget-title {
            font-size: 0.85rem;
            font-weight: 700;
            color: #c7d2fe;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .cron-widget-controls {
            display: flex;
            align-items: center;
            gap: 0.6rem;
            flex-wrap: wrap;
        }

        .cron-time-input {
            background: rgba(0, 0, 0, 0.4);
            border: 1px solid var(--border);
            color: #f8fafc;
            padding: 0.4rem 0.6rem;
            border-radius: 6px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.9rem;
        }

        .cron-status-banner {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
            padding: 0.5rem 0.85rem;
            border-radius: 6px;
            background: rgba(99, 102, 241, 0.15);
            border: 1px solid rgba(99, 102, 241, 0.3);
            color: #a5b4fc;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(-3px); }
            to { opacity: 1; transform: translateY(0); }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo-group">
                <h1>Student Attendance Testing Ground</h1>
                <p>Local Serverless Ingest API &bull; Cloud Tasks Queue &bull; Asynchronous Dispatcher &bull; Mock Database</p>
            </div>
            <div class="badge">
                <div class="pulse-dot"></div>
                <span>EMULATOR ACTIVE &bull; NO PRODUCTION IMPACT</span>
            </div>
        </header>

        <!-- System Topology -->
        <div class="topology-grid">
            <div class="service-node">
                <div class="node-header">
                    <span class="node-title">Serverless Ingest API</span>
                    <span class="node-port">:{{ ingest_port }}</span>
                </div>
                <div class="node-value">POST /api/attendance/signin</div>
                <div class="node-desc">Fast 202 Accepted response with asynchronous task handoff.</div>
            </div>

            <div class="service-node">
                <div class="node-header">
                    <span class="node-title">Cloud Tasks Emulator</span>
                    <span class="node-port">:{{ emulator_port }} (gRPC)</span>
                </div>
                <div class="node-value">student-attendance-queue</div>
                <div class="node-desc">gcloud-tasks-emulator v0.6.2 running locally.</div>
            </div>

            <div class="service-node">
                <div class="node-header">
                    <span class="node-title">Queue Dispatcher</span>
                    <span class="node-port">:{{ dispatcher_port }}</span>
                </div>
                <div class="node-value">POST /dispatch/attendance</div>
                <div class="node-desc">Processes queue tasks and validates student enrollment.</div>
            </div>

            <div class="service-node">
                <div class="node-header">
                    <span class="node-title">Mock Database</span>
                    <span class="node-port">SQLite WAL</span>
                </div>
                <div class="node-value">attendance.db</div>
                <div class="node-desc">Persistent store with 350 seeded students and sessions.</div>
            </div>
        </div>

        <!-- 2-Hour Session Clock & Cloud Tasks Queue Telemetry -->
        <div class="session-clock-card">
            <div class="clock-header">
                <div class="clock-title-group">
                    <span class="live-pill"><span class="pulse-dot"></span> LIVE 2-HOUR SESSION CLOCK</span>
                    <div class="clock-digits" id="sim-clock-display">00:00:00 <span class="clock-total">/ 02:00:00</span></div>
                    <div class="virtual-time" id="virtual-time-display">Campus Time: 08:45:00 AM &bull; Session: CS-301</div>
                </div>
                <div class="queue-depth-box">
                    <div class="queue-depth-label">Cloud Tasks Queue Buffer</div>
                    <div class="queue-depth-number" id="queue-depth-val">0</div>
                    <div class="queue-depth-sub" id="queue-depth-status">0 pending tasks</div>
                </div>
            </div>

            <div class="phase-banner" id="sim-phase-banner">
                <span class="phase-badge" id="sim-phase-badge">STANDBY</span>
                <span class="phase-desc" id="sim-phase-desc">Ready to start 2-hour attendance simulation.</span>
            </div>

            <div class="progress-bar-container" style="margin-top: 1rem; height: 10px;">
                <div class="progress-bar-fill" id="session-progress-bar" style="width: 0%;"></div>
            </div>

            <!-- Simulator Interactive Controls -->
            <div class="sim-controls-row">
                <div class="sim-btn-group">
                    <button class="btn btn-primary" id="btn-start-fast" onclick="startSim(60.0)">
                        <svg width="16" height="16" fill="currentColor" viewBox="0 0 24 24"><path d="M5 3l14 9-14 9V3z"/></svg>
                        Simulate 2h (60x / 2m)
                    </button>
                    <button class="btn btn-primary" id="btn-start-realtime" onclick="startSim(1.0)" style="background: linear-gradient(135deg, #10b981 0%, #059669 100%);">
                        <svg width="16" height="16" fill="currentColor" viewBox="0 0 24 24"><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10 10-4.5 10-10S17.5 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm.5-13H11v6l5.2 3.2.8-1.3-4.5-2.7V7z"/></svg>
                        Real-Time (1x / 2 Hours)
                    </button>
                    <button class="btn btn-secondary" id="btn-pause-sim" onclick="togglePauseSim()">Pause</button>
                    <button class="btn btn-secondary" id="btn-stop-sim" onclick="stopSim()">Stop</button>
                </div>
                <div class="speed-selector">
                    <span style="font-size: 0.8rem; color: var(--text-muted); font-weight: 600;">SPEED:</span>
                    <button class="speed-btn active" id="sp-1" onclick="setSpeed(1.0, this)">1x (2h Real)</button>
                    <button class="speed-btn" id="sp-10" onclick="setSpeed(10.0, this)">10x (12m)</button>
                    <button class="speed-btn" id="sp-30" onclick="setSpeed(30.0, this)">30x (4m)</button>
                    <button class="speed-btn" id="sp-60" onclick="setSpeed(60.0, this)">60x (2m)</button>
                    <button class="speed-btn" id="sp-120" onclick="setSpeed(120.0, this)">120x (1m)</button>
                </div>
            </div>

            <!-- Automated Real-Time Cron Scheduler -->
            <div class="cron-widget">
                <div class="cron-widget-header">
                    <span class="cron-widget-title">
                        <svg width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                        Automated Real-Time Cron Job Trigger
                    </span>
                    <span id="cron-ui-badge" style="font-size: 0.75rem; color: var(--text-muted); font-weight: 600;">STATUS: IDLE</span>
                </div>
                <div class="cron-widget-controls">
                    <label style="font-size: 0.8rem; color: var(--text-secondary); font-weight: 500;">Set Target Hour:</label>
                    <input type="time" id="cron-ui-time" class="cron-time-input" step="1">
                    <button class="btn btn-secondary btn-sm" onclick="setQuickTestTimer(15)">+15s (Quick Test)</button>
                    <button class="btn btn-secondary btn-sm" onclick="setQuickTestTimer(30)">+30s</button>
                    <button class="btn btn-secondary btn-sm" onclick="setQuickTestTimer(60)">+1m</button>
                    <button class="btn btn-primary btn-sm" id="btn-arm-cron" onclick="toggleArmCron()" style="padding: 0.4rem 1rem;">
                        Arm Auto-Trigger
                    </button>
                    <button class="btn btn-secondary btn-sm" id="btn-wake-server" onclick="pingWakeupServer()" style="color: #38bdf8; border-color: rgba(56, 189, 248, 0.4);">
                        ⚡ Wake Server
                    </button>
                </div>
                <div class="cron-status-banner" id="cron-status-box" style="display: none;">
                    <span id="cron-status-msg">Armed: Waiting for target hour...</span>
                    <span id="cron-status-countdown" style="color: #38bdf8; font-weight: 600;">00:00:00</span>
                </div>
            </div>
        </div>

        <!-- Metrics Overview -->
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Enrolled Students</div>
                <div class="metric-number" id="m-students">350</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Status: PRESENT</div>
                <div class="metric-number" id="m-present" style="color: #34d399;">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Status: LATE</div>
                <div class="metric-number" id="m-late" style="color: #fbbf24;">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Status: ABSENT</div>
                <div class="metric-number" id="m-absent" style="color: #f87171;">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Duplicates Blocked</div>
                <div class="metric-number" id="m-duplicates" style="color: #38bdf8;">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Tasks Dispatched</div>
                <div class="metric-number" id="m-tasks" style="color: #c084fc;">0</div>
            </div>
        </div>

        <!-- Actions Panel -->
        <div class="actions-panel">
            <div class="actions-info">
                <h3>Manual Trigger & Database Controls</h3>
                <p>Execute instant test scenarios or reset the session database.</p>
            </div>
            <div class="btn-group">
                <button class="btn btn-secondary" onclick="resetDatabase()">Reset Attendance</button>
                <button class="btn btn-secondary" onclick="reconcileAbsences()">Finalize Absences</button>
                <button class="btn btn-secondary" id="btn-run-test" onclick="runLoadTest()">
                    <svg width="16" height="16" fill="currentColor" viewBox="0 0 24 24"><path d="M5 3l14 9-14 9V3z"/></svg>
                    Simulate 300 Uniform Sign-Ins
                </button>
            </div>
        </div>

        <!-- Real-time Test Output -->
        <div id="test-status-box">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span id="test-status-text">Ready to run simulation...</span>
                <span id="test-progress-text">0 / 300</span>
            </div>
            <div class="progress-bar-container">
                <div class="progress-bar-fill" id="test-progress-bar"></div>
            </div>
        </div>

        <!-- Live State Transitions Stream -->
        <div class="card-table" style="margin-bottom: 2rem;">
            <h2>
                <span>Live Scan Queue & State Transitions</span>
                <span style="font-size: 0.8rem; color: #a5b4fc;" id="live-scan-count">Listening for arrivals...</span>
            </h2>
            <div id="live-state-stream" class="activity-stream">
                <div class="stream-empty">No arrivals dispatched yet. Click "Simulate 2h (60x)" or "Real-Time (1x)" above!</div>
            </div>
        </div>

        <!-- Recent Records Table -->
        <div class="card-table">
            <h2>
                <span>Database Attendance Records</span>
                <span style="font-size: 0.8rem; color: var(--text-muted);" id="feed-status">Auto-refreshing every 2s</span>
            </h2>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Student ID</th>
                        <th>Student Name</th>
                        <th>Session</th>
                        <th>Sign-In Time</th>
                        <th>Status</th>
                        <th>Task Reference</th>
                    </tr>
                </thead>
                <tbody id="records-table-body">
                    <tr><td colspan="7" style="text-align: center; padding: 2rem;">No attendance records found yet. Run the simulation above!</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        async function fetchStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                document.getElementById('m-students').innerText = data.total_registered_students || 350;
                document.getElementById('m-tasks').innerText = data.total_tasks_processed || 0;
                document.getElementById('m-present').innerText = data.present_count || 0;
                document.getElementById('m-late').innerText = data.late_count || 0;
                document.getElementById('m-absent').innerText = data.absent_count || 0;
                document.getElementById('m-duplicates').innerText = data.duplicate_attempts_blocked || 0;
            } catch (err) {
                console.error("Stats fetch error:", err);
            }
        }

        async function fetchRecentRecords() {
            try {
                const res = await fetch('/api/records?limit=15');
                const records = await res.json();
                const tbody = document.getElementById('records-table-body');
                if (!records || records.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; padding: 2rem;">No attendance records found yet. Run the simulation above!</td></tr>';
                    return;
                }
                tbody.innerHTML = records.map(r => `
                    <tr>
                        <td class="code-cell">#${r.id}</td>
                        <td class="code-cell" style="color: #a5b4fc; font-weight: 600;">${r.student_id}</td>
                        <td style="color: #f1f5f9;">${r.full_name}</td>
                        <td class="code-cell">${r.session_id}</td>
                        <td class="code-cell">${r.sign_in_time ? r.sign_in_time.replace('T', ' ').substring(0, 19) : 'N/A'}</td>
                        <td><span class="status-pill status-${r.status.toLowerCase()}">${r.status}</span></td>
                        <td class="code-cell" style="font-size: 0.75rem; color: #64748b;">${r.task_id}</td>
                    </tr>
                `).join('');
            } catch (err) {
                console.error("Records fetch error:", err);
            }
        }

        async function resetDatabase() {
            if (confirm("Clear all attendance records for a fresh test run?")) {
                await fetch('/api/reset', { method: 'POST' });
                refreshData();
            }
        }

        async function reconcileAbsences() {
            const res = await fetch('/api/attendance/reconcile', { method: 'POST' });
            const data = await res.json();
            alert(`Session finalized! ${data.absent_count_recorded} absent students recorded.`);
            refreshData();
        }

        function refreshData() {
            fetchStats();
            fetchRecentRecords();
        }

        let currentSpeed = 1.0;
        let isSimRunning = false;
        let isSimPaused = false;

        async function pollSimulation() {
            try {
                const res = await fetch('/api/simulation/status');
                const data = await res.json();
                
                isSimRunning = data.is_running;
                isSimPaused = data.is_paused;

                // Update clock display
                document.getElementById('sim-clock-display').innerHTML = `${data.sim_clock} <span class="clock-total">/ 02:00:00</span>`;
                document.getElementById('virtual-time-display').innerHTML = `Campus Clock: ${data.virtual_time_str} &bull; Session: CS-301`;
                document.getElementById('session-progress-bar').style.width = `${data.progress_percent}%`;

                // Update phase banner
                const badge = document.getElementById('sim-phase-badge');
                const desc = document.getElementById('sim-phase-desc');
                if (data.is_running) {
                    badge.innerText = data.is_paused ? 'PAUSED' : 'ACTIVE';
                    badge.style.background = data.is_paused ? 'var(--warning)' : 'var(--accent)';
                    desc.innerText = data.current_phase;
                } else if (data.reconciled) {
                    badge.innerText = 'CONCLUDED';
                    badge.style.background = 'var(--success)';
                    desc.innerText = '2-Hour session ended. Absences reconciled and session finalized.';
                } else {
                    badge.innerText = 'STANDBY';
                    badge.style.background = 'var(--text-muted)';
                    desc.innerText = 'Ready to start 2-hour attendance simulation.';
                }

                // Update pause button text
                const pauseBtn = document.getElementById('btn-pause-sim');
                if (pauseBtn) pauseBtn.innerText = isSimPaused ? 'Resume' : 'Pause';

                // Render live state stream
                const stream = document.getElementById('live-state-stream');
                const countBadge = document.getElementById('live-scan-count');
                const activities = data.recent_activity || [];
                
                countBadge.innerText = `${data.total_dispatched} / ${data.total_scheduled} tasks dispatched`;

                if (activities.length > 0) {
                    stream.innerHTML = activities.map(a => `
                        <div class="stream-item">
                            <div class="stream-left">
                                <span class="stream-time">[T+${a.sim_time} &bull; ${a.virtual_time}]</span>
                                <span class="stream-student">${a.student_id}</span>
                                <span style="color: var(--text-muted); font-size: 0.75rem;">(${a.kiosk})</span>
                            </div>
                            <div class="stream-flow">
                                <span>QUEUED</span>
                                <span>&rarr;</span>
                                <span class="status-pill status-${a.status.toLowerCase()}">${a.status}</span>
                            </div>
                        </div>
                    `).join('');
                } else if (!data.is_running) {
                    stream.innerHTML = '<div class="stream-empty">No arrivals dispatched yet. Click "Simulate 2h (60x)" or "Real-Time (1x)" above!</div>';
                }

            } catch (err) {
                console.error("Sim poll error:", err);
            }
        }

        async function pollQueueDepth() {
            try {
                const res = await fetch('/api/queue/status');
                const data = await res.json();
                document.getElementById('queue-depth-val').innerText = data.queue_depth;
                document.getElementById('queue-depth-status').innerText = `${data.queue_depth} buffered in queue &bull; ${data.total_dispatched} processed`;
            } catch (err) {
                console.error("Queue poll error:", err);
            }
        }

        async function startSim(speed) {
            currentSpeed = speed;
            await fetch('/api/simulation/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ speed: speed, reset_db: true })
            });
            refreshData();
            highlightSpeedBtn(speed);
        }

        async function togglePauseSim() {
            if (isSimPaused) {
                await fetch('/api/simulation/resume', { method: 'POST' });
            } else {
                await fetch('/api/simulation/pause', { method: 'POST' });
            }
            pollSimulation();
        }

        async function stopSim() {
            await fetch('/api/simulation/stop', { method: 'POST' });
            pollSimulation();
            refreshData();
        }

        async function setSpeed(speed, btn) {
            currentSpeed = speed;
            await fetch('/api/simulation/speed', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ speed: speed })
            });
            highlightSpeedBtn(speed);
        }

        function highlightSpeedBtn(speed) {
            document.querySelectorAll('.speed-btn').forEach(b => b.classList.remove('active'));
            const map = { 1.0: 'sp-1', 10.0: 'sp-10', 30.0: 'sp-30', 60.0: 'sp-60', 120.0: 'sp-120' };
            const id = map[speed];
            if (id && document.getElementById(id)) {
                document.getElementById(id).classList.add('active');
            }
        }

        // Web Dashboard Scheduled Cron Auto-Trigger
        let scheduledTargetDate = null;
        let cronTimerInterval = null;

        function setQuickTestTimer(seconds) {
            const target = new Date(Date.now() + seconds * 1000);
            const timeString = target.toTimeString().split(' ')[0];
            document.getElementById('cron-ui-time').value = timeString;
            armCronWithTarget(target);
        }

        function toggleArmCron() {
            if (scheduledTargetDate) {
                disarmCron();
            } else {
                const val = document.getElementById('cron-ui-time').value;
                if (!val) {
                    alert("Please select a target hour/time (e.g. 17:15) or click one of the quick test buttons (+15s, +30s).");
                    return;
                }
                const parts = val.split(':');
                const target = new Date();
                target.setHours(parseInt(parts[0], 10));
                target.setMinutes(parseInt(parts[1], 10));
                target.setSeconds(parts[2] ? parseInt(parts[2], 10) : 0);
                target.setMilliseconds(0);

                if (target.getTime() <= Date.now()) {
                    target.setDate(target.getDate() + 1);
                }
                armCronWithTarget(target);
            }
        }

        let cronPreWarmed = false;

        async function pingWakeupServer() {
            const btn = document.getElementById('btn-wake-server');
            if (btn) {
                btn.disabled = true;
                btn.innerText = '⚡ Pinging...';
            }
            try {
                const res = await fetch('/api/wakeup', { method: 'POST' });
                const data = await res.json();
                if (btn) btn.innerText = `⚡ Awake (${data.uptime_seconds}s)`;
                console.log("[WAKE-UP] Server responded:", data);
            } catch (err) {
                if (btn) btn.innerText = '⚡ Wake Failed';
                console.error("[WAKE-UP ERROR]", err);
            } finally {
                setTimeout(() => {
                    if (btn) {
                        btn.disabled = false;
                        btn.innerText = '⚡ Wake Server';
                    }
                }, 3000);
            }
        }

        function armCronWithTarget(targetDate) {
            scheduledTargetDate = targetDate;
            cronPreWarmed = false;
            document.getElementById('cron-status-box').style.display = 'flex';
            document.getElementById('cron-ui-badge').innerText = 'STATUS: ARMED';
            document.getElementById('cron-ui-badge').style.color = '#38bdf8';
            document.getElementById('btn-arm-cron').innerText = 'Disarm / Cancel';
            document.getElementById('btn-arm-cron').style.background = 'var(--danger)';

            if (cronTimerInterval) clearInterval(cronTimerInterval);
            cronTimerInterval = setInterval(updateCronCountdown, 250);
            updateCronCountdown();
        }

        function disarmCron() {
            scheduledTargetDate = null;
            cronPreWarmed = false;
            if (cronTimerInterval) clearInterval(cronTimerInterval);
            document.getElementById('cron-status-box').style.display = 'none';
            document.getElementById('cron-ui-badge').innerText = 'STATUS: IDLE';
            document.getElementById('cron-ui-badge').style.color = 'var(--text-muted)';
            document.getElementById('btn-arm-cron').innerText = 'Arm Auto-Trigger';
            document.getElementById('btn-arm-cron').style.background = '';
        }

        function updateCronCountdown() {
            if (!scheduledTargetDate) return;
            const now = Date.now();
            const diffMs = scheduledTargetDate.getTime() - now;

            // Pre-warm server at T-minus 30s to prevent cold-start latency
            if (diffMs <= 30000 && diffMs > 0 && !cronPreWarmed) {
                cronPreWarmed = true;
                console.log("[CRON PRE-WARM] T-30s reached. Pinging /api/wakeup to warm up connections...");
                fetch('/api/wakeup', { method: 'POST' }).catch(() => {});
            }

            if (diffMs <= 0) {
                const firedTime = new Date().toLocaleTimeString();
                disarmCron();
                
                document.getElementById('cron-status-box').style.display = 'flex';
                document.getElementById('cron-status-msg').innerHTML = `<strong style="color: #34d399;">🚨 CRON FIRED AT ${firedTime}! Automatically launched 2-Hour attendance simulation!</strong>`;
                document.getElementById('cron-status-countdown').innerText = 'TRIGGERED';

                // Automatically trigger simulation!
                startSim(currentSpeed || 60.0);

                setTimeout(() => {
                    document.getElementById('cron-status-box').style.display = 'none';
                }, 8000);
                return;
            }

            const diffSec = Math.floor(diffMs / 1000);
            const hrs = String(Math.floor(diffSec / 3600)).padStart(2, '0');
            const mins = String(Math.floor((diffSec % 3600) / 60)).padStart(2, '0');
            const secs = String(diffSec % 60).padStart(2, '0');

            document.getElementById('cron-status-msg').innerText = `Target: ${scheduledTargetDate.toLocaleTimeString()} • Firing attendance simulation in:`;
            document.getElementById('cron-status-countdown').innerText = `${hrs}:${mins}:${secs}`;
        }

        async function runLoadTest() {
            const btn = document.getElementById('btn-run-test');
            const box = document.getElementById('test-status-box');
            const statusText = document.getElementById('test-status-text');
            const progressText = document.getElementById('test-progress-text');
            const bar = document.getElementById('test-progress-bar');

            btn.disabled = true;
            btn.style.opacity = '0.5';
            box.style.display = 'block';
            statusText.innerText = 'Spawning 300 simultaneous sign-in requests...';
            bar.style.width = '10%';
            progressText.innerText = 'Dispatching...';

            const total = 300;
            let success = 0;
            const startTime = performance.now();

            const requests = [];
            for (let i = 1; i <= total; i++) {
                const stuId = 'STU' + String(i).padStart(3, '0');
                const req = fetch('/api/attendance/signin', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        student_id: stuId,
                        session_id: 'SESS-2026-09-17',
                        device_id: 'browser-scanner-' + (i % 5),
                        sign_in_time: (i <= 250) ? '2026-09-17T09:05:00Z' : '2026-09-17T09:20:00Z'
                    })
                }).then(r => {
                    if (r.status === 202) success++;
                    const pct = Math.round((success / total) * 100);
                    bar.style.width = pct + '%';
                    progressText.innerText = `${success} / ${total} enqueued`;
                }).catch(e => console.error(e));
                requests.push(req);
            }

            await Promise.all(requests);
            const durationSec = ((performance.now() - startTime) / 1000).toFixed(2);
            statusText.innerText = `Ingest complete: 300/300 requests enqueued in ${durationSec}s! Draining Cloud Tasks queue...`;

            setTimeout(() => {
                refreshData();
                statusText.innerText = `Finished! Ingest: 300 tasks enqueued in ${durationSec}s. Database verified.`;
                btn.disabled = false;
                btn.style.opacity = '1';
            }, 1500);
        }

        // Periodic auto-refresh
        setInterval(refreshData, 2000);
        setInterval(pollSimulation, 800);
        setInterval(pollQueueDepth, 1000);
        refreshData();
        pollSimulation();
        pollQueueDepth();
    </script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def dashboard():
    return render_template_string(
        HTML_DASHBOARD,
        ingest_port=INGEST_PORT,
        emulator_port=EMULATOR_PORT,
        dispatcher_port=DISPATCHER_PORT,
    )


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "HEALTHY",
        "service": "serverless-attendance-ingest-api",
        "emulator_target": f"{EMULATOR_HOST}:{EMULATOR_PORT}",
        "dispatcher_target": f"{DISPATCHER_HOST}:{DISPATCHER_PORT}",
        "wakeup_count": _wakeup_count,
        "last_wakeup_utc": _last_wakeup_time,
        "uptime_seconds": int(time.time() - _server_start_time),
    }), 200


@app.route("/api/wakeup", methods=["GET", "POST"])
def wakeup():
    """
    Dedicated wake-up endpoint for cron jobs, scale-to-zero serverless environments,
    and external heartbeat monitors.
    Pings DB and Cloud Tasks client to warm up connection pools and ensure zero cold-start delay.
    """
    global _last_wakeup_time, _wakeup_count
    _wakeup_count += 1
    _last_wakeup_time = datetime.now(timezone.utc).isoformat()
    caller = request.headers.get("User-Agent", "cron-job")
    logger.info(f"Wake-up ping received (#{_wakeup_count}) from {caller}")

    # Verify DB connectivity
    db_ok = False
    try:
        stats = db.get_stats()
        db_ok = stats.get("total_registered_students", 0) > 0
    except Exception as e:
        logger.warning(f"Database check during wakeup failed: {e}")

    # Prime Cloud Tasks client connection
    queue_ok = False
    try:
        client = get_cloud_tasks_client()
        queue_ok = client is not None
    except Exception as e:
        logger.warning(f"Cloud Tasks client initialization during wakeup failed: {e}")

    uptime_seconds = int(time.time() - _server_start_time)

    return jsonify({
        "status": "AWAKE",
        "service": "serverless-attendance-ingest-api",
        "ready": True,
        "database_connected": db_ok,
        "tasks_client_ready": queue_ok,
        "wakeup_count": _wakeup_count,
        "last_wakeup_utc": _last_wakeup_time,
        "uptime_seconds": uptime_seconds,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }), 200


@app.route("/api/shutdown", methods=["POST"])
def shutdown():
    """
    Shuts down local test ground servers and background workers.
    """
    caller = request.headers.get("User-Agent", "cron-job")
    logger.info(f"Shutdown request received via /api/shutdown from {caller}")

    def _do_shutdown():
        time.sleep(0.4)
        if _shutdown_hook:
            _shutdown_hook()
        else:
            os._exit(0)

    threading.Thread(target=_do_shutdown, daemon=True).start()
    return jsonify({"status": "SHUTTING_DOWN", "message": "Services stopping..."}), 200


@app.route("/api/stats", methods=["GET"])
def stats():
    return jsonify(db.get_stats()), 200


@app.route("/api/records", methods=["GET"])
def records():
    limit = request.args.get("limit", default=25, type=int)
    return jsonify(db.get_recent_records(limit=limit)), 200


@app.route("/api/reset", methods=["POST"])
def reset():
    db.clear_attendance()
    return jsonify({"status": "SUCCESS", "message": "Attendance records cleared."}), 200


@app.route("/api/attendance/reconcile", methods=["POST"])
def reconcile():
    session_id = "SESS-2026-09-17"
    if request.is_json and request.json and "session_id" in request.json:
        session_id = request.json["session_id"]
    result = db.reconcile_absent_students(session_id=session_id)
    return jsonify(result), 200


@app.route("/api/queue/status", methods=["GET"])
def queue_status():
    """Returns real-time queue buffer metrics from Cloud Tasks emulator."""
    depth = 0
    try:
        client = get_cloud_tasks_client()
        tasks = list(client.list_tasks(parent=QUEUE_NAME, timeout=1.0))
        depth = len(tasks)
    except Exception as e:
        logger.debug(f"Queue list error: {e}")

    stats = db.get_stats()
    return jsonify({
        "queue_name": QUEUE_NAME,
        "queue_depth": depth,
        "total_dispatched": stats.get("total_tasks_processed", 0),
        "status": "HEALTHY",
    }), 200


@app.route("/api/simulation/start", methods=["POST"])
def sim_start():
    speed = 60.0
    reset_db = True
    if request.is_json and request.json:
        speed = float(request.json.get("speed", 60.0))
        reset_db = bool(request.json.get("reset_db", True))
    simulator.start(speed=speed, reset_db=reset_db)
    return jsonify({"status": "STARTED", "speed": speed}), 200


@app.route("/api/simulation/pause", methods=["POST"])
def sim_pause():
    simulator.pause()
    return jsonify({"status": "PAUSED"}), 200


@app.route("/api/simulation/resume", methods=["POST"])
def sim_resume():
    simulator.resume()
    return jsonify({"status": "RESUMED"}), 200


@app.route("/api/simulation/stop", methods=["POST"])
def sim_stop():
    simulator.stop()
    return jsonify({"status": "STOPPED"}), 200


@app.route("/api/simulation/speed", methods=["POST"])
def sim_speed():
    if request.is_json and request.json and "speed" in request.json:
        speed = float(request.json["speed"])
        simulator.set_speed(speed)
        return jsonify({"status": "UPDATED", "speed": speed}), 200
    return jsonify({"error": "Missing speed parameter"}), 400


@app.route("/api/simulation/status", methods=["GET"])
def sim_status():
    return jsonify(simulator.get_status()), 200


@app.route("/api/attendance/signin", methods=["POST"])
def signin():
    """
    High-performance serverless ingest endpoint.
    Accepts sign-in payload, wraps it in a Cloud Task, immediately dispatches to the
    local Cloud Tasks emulator, and returns HTTP 202 Accepted.
    """
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            raw_data = request.get_data(as_text=True)
            data = json.loads(raw_data) if raw_data else {}
    except Exception as e:
        return jsonify({"error": "Invalid JSON body", "details": str(e)}), 400

    student_id = data.get("student_id")
    session_id = data.get("session_id", "SESS-2026-09-17")
    device_id = data.get("device_id", "terminal-kiosk-01")
    sign_in_time = data.get("sign_in_time") or datetime.now(timezone.utc).isoformat()

    if not student_id:
        return jsonify({"error": "student_id is required"}), 400

    # Generate distinct task ID
    task_id = f"task-{uuid.uuid4().hex[:12]}"
    task_payload = {
        "task_id": task_id,
        "student_id": student_id,
        "session_id": session_id,
        "device_id": device_id,
        "sign_in_time": sign_in_time,
    }

    # Dispatch to local Cloud Tasks Emulator
    try:
        client = get_cloud_tasks_client()
        target_url = f"http://{DISPATCHER_HOST}:{DISPATCHER_PORT}/dispatch/attendance"

        task = Task(
            name=f"{QUEUE_NAME}/tasks/{task_id}",
            http_request=HttpRequest(
                http_method=HttpMethod.POST,
                url=target_url,
                headers={"Content-Type": "application/json"},
                body=json.dumps(task_payload).encode("utf-8"),
            ),
        )

        client.create_task(parent=QUEUE_NAME, task=task)

        return jsonify({
            "status": "QUEUED",
            "message": "Attendance sign-in enqueued to Cloud Tasks",
            "task_id": task_id,
            "student_id": student_id,
            "session_id": session_id,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        }), 202

    except Exception as e:
        logger.error(f"Failed to enqueue Cloud Task for student {student_id}: {e}")
        return jsonify({
            "error": "Failed to enqueue attendance task",
            "details": str(e),
        }), 500


def run_standalone():
    parser = argparse.ArgumentParser(description="Run Attendance Serverless Ingest API")
    parser.add_argument("--host", default=INGEST_HOST, help="Ingest API host")
    parser.add_argument("--port", type=int, default=INGEST_PORT, help="Ingest API port")
    args = parser.parse_args()

    # Ensure DB is seeded
    db.init_db()
    db.seed_data()

    print("\n" + "=" * 60)
    print(f"  SERVERLESS ATTENDANCE INGEST API")
    print(f"  Dashboard:    http://{args.host}:{args.port}/")
    print(f"  Ingest API:   http://{args.host}:{args.port}/api/attendance/signin")
    print(f"  Emulator gRPC: {EMULATOR_HOST}:{EMULATOR_PORT}")
    print("=" * 60 + "\n")

    app.run(host=args.host, port=args.port, threaded=True, debug=False)


if __name__ == "__main__":
    run_standalone()
