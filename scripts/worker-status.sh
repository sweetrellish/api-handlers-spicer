#!/bin/bash
# Check status of Marketsharp workers both background and UI 

systemctl status marketsharp_queue_worker.service
systemctl status spicer-worker.service
