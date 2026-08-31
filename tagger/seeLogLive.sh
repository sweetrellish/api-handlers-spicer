#!/bin/bash
# Run logs of comment mention worker

journalctl -u marketsharp_comment_worker.service -f
